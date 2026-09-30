WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, attr_value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM flattened
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        LEAD(island_id, 1) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key) AS next_island_id,
        NTILE(10) OVER (PARTITION BY attr_key ORDER BY island_id, id, attr_key) AS island_id_bucket,
        SUM(island_id) OVER w_recent AS island_id_sum_30m
    FROM sessionized
    WINDOW w_recent AS (PARTITION BY attr_key, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        next_island_id,
        island_id_bucket,
        LEAD(island_id_sum_30m) OVER w_recent AS next_island_id_sum_30m,
        LAG(island_id_bucket) OVER w_recent AS prev_island_id_bucket,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM enriched
    WHERE attr_key IN ('utm_source', 'gift')
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY created_at, id, attr_key)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_part AS event_seq_2,
        SUM(island_id_bucket) OVER (PARTITION BY attr_key, customer_id) AS attr_key_sum_island_id_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY next_island_id_sum_30m DESC NULLS FIRST) AS next_island_id_sum_30m_cume_dist,
        AVG(event_seq) OVER (PARTITION BY customer_id ORDER BY island_id_bucket, id, attr_key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_seq_avg_6_prior
    FROM scored
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY prev_island_id_bucket, id, attr_key), w_ordered AS (PARTITION BY attr_key ORDER BY island_id_bucket, id, attr_key)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        attr_key,
        next_island_id_sum_30m,
        prev_island_id_bucket,
        event_seq_2,
        attr_key_sum_island_id_bucket,
        next_island_id_sum_30m_cume_dist,
        event_seq_avg_6_prior,
        SUM(event_seq_avg_6_prior) OVER (PARTITION BY customer_id ORDER BY event_seq_avg_6_prior, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY customer_id, attr_key ORDER BY event_seq_avg_6_prior DESC, id, attr_key) AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_seq_2, id, attr_key) AS event_seq_3
    FROM metrics
)
SELECT
    s.id,
    s.customer_id,
    s.attr_key,
    s.prev_island_id_bucket,
    s.attr_key_sum_island_id_bucket,
    s.next_island_id_sum_30m_cume_dist,
    s.event_seq_avg_6_prior,
    s.seq_no_2,
    CASE WHEN attr_key RLIKE '^(device|utm_source)$' THEN 'CORE' WHEN attr_key RLIKE '^(gift|weight_kg)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS attr_key_zone,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM with_windows s
ORDER BY min_account_balance NULLS LAST;

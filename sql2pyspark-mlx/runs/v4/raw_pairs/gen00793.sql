WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER w_grp AS seq_no,
        ROW_NUMBER() OVER w_running AS event_seq_2,
        ROW_NUMBER() OVER (ORDER BY created_at, id, attr_key) AS row_seq
    FROM flattened
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY created_at DESC, id, attr_key), w_running AS (PARTITION BY attr_key ORDER BY created_at, id, attr_key)
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value,
        event_seq,
        seq_no,
        event_seq_2,
        row_seq,
        AVG(row_seq) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS row_seq_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY event_seq_2, id, attr_key) AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY event_seq DESC, id, attr_key) AS seq_no_3
    FROM enriched
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value,
        event_seq,
        seq_no,
        event_seq_2,
        row_seq,
        row_seq_avg_3_prior,
        seq_no_2,
        seq_no_3,
        SUM(row_seq_avg_3_prior) OVER (ORDER BY row_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq_avg_3_prior,
        AVG(event_seq_2) OVER (PARTITION BY attr_key) AS attr_key_avg_event_seq_2,
        AVG(seq_no) OVER (PARTITION BY attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS seq_no_avg_2h
    FROM enriched_carts
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(created_at) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key) AS prev_created_at
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_created_at IS NULL OR created_at > prev_created_at + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', prev_created_at) AS year_start,
        COUNT(*) FILTER (WHERE attr_key = 'device') AS device_rows,
        COUNT(*) AS row_count,
        COUNT(CASE WHEN attr_key = 'weight_kg' THEN 1 END) AS weight_kg_count,
        COUNT_IF(attr_key = 'gift') AS gift_events,
        COUNT_IF(attr_key = 'utm_source') AS utm_source_events,
        MAX(row_seq_avg_3_prior) FILTER (WHERE attr_key = 'gift') AS gift_max_row_seq_avg_3_prior
    FROM islands
    GROUP BY 1
)
SELECT
    year_start,
    device_rows,
    row_count,
    weight_kg_count,
    gift_events,
    gift_max_row_seq_avg_3_prior
FROM stats
ORDER BY row_count DESC NULLS LAST;

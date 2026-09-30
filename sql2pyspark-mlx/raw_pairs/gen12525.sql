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
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, attr_value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, attr_key ORDER BY created_at, id, attr_key) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY created_at DESC, id, attr_key) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY created_at, id, attr_key) AS seq_no_2
    FROM flattened
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        attr_key,
        attr_value,
        seq_no_2,
        CEIL(seq_no) AS seq_no_whole,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM enriched
    WHERE seq_no BETWEEN 5 AND 6
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT
    SELECT id
    FROM carts
    WHERE created_at >= '2024-02-24'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER w_grp AS seq_no_whole_percent_rank,
        MAX(seq_no_whole) OVER (PARTITION BY TO_DATE(created_at), attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS seq_no_whole_max_15m,
        COUNT(seq_no_2) OVER w_running AS seq_no_2_count_1h,
        AVG(seq_no_whole) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS seq_no_whole_avg_30m
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY seq_no_whole DESC NULLS LAST), w_running AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('WEEK', created_at) AS created_at_period
    FROM enriched_carts
    WHERE attr_key IS NOT NULL
      AND created_at > TIMESTAMP '2025-03-01 19:00:00'
),
enriched_carts_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        attr_key_clean,
        seq_no_whole_avg_30m,
        created_at_period,
        ROUND(seq_no_2_count_1h / SUM(seq_no_2_count_1h) OVER (PARTITION BY attr_key), 4) AS seq_no_2_count_1h_share
    FROM cleaned
    WHERE attr_key != 'weight_kg'
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no_whole_avg_30m >= 73 THEN 'HIGH' ELSE 'LOW' END AS seq_no_whole_avg_30m_tier,
    ROUND(AVG(seq_no_2_count_1h_share), 2) AS avg_seq_no_2_count_1h_share,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched_carts_2
GROUP BY CASE WHEN seq_no_whole_avg_30m >= 73 THEN 'HIGH' ELSE 'LOW' END
HAVING distinct_ids < 1000
ORDER BY avg_seq_no_2_count_1h_share DESC

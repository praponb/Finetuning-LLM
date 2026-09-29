WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        attr_key,
        v,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE attr_key <> 'region'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        v,
        status_code_high,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY flag_bit1 DESC) AS flag_bit1_percent_rank,
        PERCENT_RANK() OVER w_grp AS status_code_high_percent_rank
    FROM unpacked
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY status_code_high DESC)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status_code_high_percent_rank >= 0.63 THEN 'High' WHEN status_code_high_percent_rank >= 0.22 THEN 'Medium' ELSE 'Low' END AS status_code_high_percent_rank_level
    FROM enriched
    WHERE status_code_high_percent_rank <= 0.52
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        v,
        status_code_high,
        status_code_high_percent_rank,
        status_code_high_percent_rank_level,
        flag_bit1_percent_rank * status_code_high AS flag_bit1_percent_rank_times_status_code_high,
        flag_bit1_percent_rank + status_code_high_percent_rank AS flag_bit1_percent_rank_plus_status_code_high_percent_rank
    FROM filtered
    WHERE flag_bit1_percent_rank < 0.41
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        status_code_high_percent_rank_level,
        flag_bit1_percent_rank_plus_status_code_high_percent_rank,
        RANK() OVER (ORDER BY status_code_high) AS status_code_high_rank,
        DENSE_RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_dense_rank
    FROM valid
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(status_code_high_dense_rank, 1) AS status_code_high_dense_rank_rounded
    FROM with_windows
    WHERE flag_bit1_percent_rank_plus_status_code_high_percent_rank BETWEEN 29 AND 59
)
-- Grouped metrics
SELECT
    CASE WHEN flag_bit1_percent_rank_plus_status_code_high_percent_rank >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS flag_bit1_percent_rank_plus_status_code_high_percent_rank_tier,
    CASE WHEN status_code_high_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_dense_rank_tier,
    ROUND(AVG(status_code_high_rank), 2) AS avg_status_code_high_rank,
    MAX(status_code_high_rank) AS max_status_code_high_rank,
    GROUPING_ID() AS grouping_id
FROM valid_app_logs
GROUP BY GROUPING SETS ((CASE WHEN flag_bit1_percent_rank_plus_status_code_high_percent_rank >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_code_high_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN flag_bit1_percent_rank_plus_status_code_high_percent_rank >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY avg_status_code_high_rank DESC;

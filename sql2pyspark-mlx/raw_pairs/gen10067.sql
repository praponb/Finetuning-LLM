WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN duration_sec >= 538 THEN 'High' WHEN duration_sec >= 107 THEN 'Medium' ELSE 'Low' END AS duration_sec_band,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-04-15 14:00:00')
      AND page IS NOT DISTINCT FROM '/product'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT event_type, page
    FROM base
    EXCEPT ALL
    SELECT event_type, page
    FROM web_events
    WHERE event_time >= '2023-06-27'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.event_type = k.event_type AND s.page = k.page
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_count_90d,
        RANK() OVER (ORDER BY duration_sec NULLS LAST) AS duration_sec_rank
    FROM reconciled
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(duration_sec_count_90d) OVER (PARTITION BY event_type) AS event_type_count_duration_sec_count_90d,
        DENSE_RANK() OVER (PARTITION BY MONTH(event_time) ORDER BY duration_sec_count_90d DESC NULLS FIRST) AS duration_sec_count_90d_dense_rank,
        MAX(duration_sec_rank) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_rank_max_90d
    FROM with_windows
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.country,
        d.device,
        d.browser
    FROM metrics src
    INNER JOIN sessions d
        ON src.session_id = d.id
    WHERE d.customer_id IN (29, 33, 34)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    CASE WHEN duration_sec_count_90d_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_count_90d_dense_rank_tier,
    device,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows,
    COUNT_IF(event_type = 'click') AS click_events,
    PERCENTILE_APPROX(duration_sec_count_90d_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_count_90d_dense_rank_quartiles
FROM joined
GROUP BY year_start, duration_sec_count_90d_dense_rank_tier, device
HAVING purchase_rows > 1;

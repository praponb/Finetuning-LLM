WITH scoped_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(rate, 0.0) AS rate_filled
    FROM tariff_windows
    WHERE tariff NOT IN ('FLAT', 'TOU')
      AND rate > 2.2
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(tariff), 1, 4) AS tariff_prefix,
        CASE WHEN rate_filled >= 5.9 THEN 'HIGH' WHEN rate_filled >= 4.1 THEN 'MEDIUM' ELSE 'LOW' END AS rate_filled_tier
    FROM scoped_tariffs
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', valid_to) AS valid_to_month,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(rate) AS max_rate
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', valid_to)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM per_group
    WHERE distinct_ids < (SELECT MAX(distinct_ids) FROM per_group)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        valid_to_month,
        n_rows,
        ROUND(CUME_DIST() OVER (PARTITION BY n_rows ORDER BY max_rate ASC NULLS LAST), 3) AS max_rate_cume_dist,
        ROUND(max_rate - AVG(max_rate) OVER (PARTITION BY n_rows, distinct_ids), 2) AS max_rate_vs_n_rows_distinct_ids_avg,
        COUNT(*) OVER (PARTITION BY n_rows, distinct_ids) AS rows_in_n_rows_distinct_ids
    FROM filtered
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE rows_in_n_rows_distinct_ids > (SELECT AVG(rows_in_n_rows_distinct_ids) FROM with_windows)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE max_rate_cume_dist > (SELECT AVG(max_rate_cume_dist) FROM with_activity)
)
SELECT
    valid_to_month,
    n_rows,
    max_rate_cume_dist,
    max_rate_vs_n_rows_distinct_ids_avg,
    rows_in_n_rows_distinct_ids,
    ROUND(max_rate_cume_dist / NULLIF(n_rows, 0), 3) AS max_rate_cume_dist_per_n_rows
FROM qualified
ORDER BY max_rate_cume_dist_per_n_rows DESC

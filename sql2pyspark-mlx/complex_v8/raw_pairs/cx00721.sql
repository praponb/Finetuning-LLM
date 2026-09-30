WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.kwh) AS prior_kwh_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
rollup_base AS (
    -- Summarise per group
    SELECT prior_rows_7d, COUNT(*) AS row_count, COUNT(DISTINCT id) AS distinct_ids
    FROM matched
    GROUP BY prior_rows_7d
    HAVING AVG(prior_kwh_7d) > 60.0
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        prior_rows_7d,
        row_count,
        ROUND(CUME_DIST() OVER w_distinct_ids_prior_rows_7d_b, 3) AS row_count_cume_dist,
        SUM(row_count) OVER (PARTITION BY prior_rows_7d ORDER BY distinct_ids, prior_rows_7d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_count,
        ROUND(prior_rows_7d - AVG(prior_rows_7d) OVER w_row_count_distinct_ids, 2) AS prior_rows_7d_vs_row_count_distinct_ids_avg
    FROM rollup_base
    WINDOW w_distinct_ids_prior_rows_7d_b AS (PARTITION BY distinct_ids, prior_rows_7d ORDER BY row_count ASC), w_row_count_distinct_ids AS (PARTITION BY row_count, distinct_ids)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        prior_rows_7d - LAG(prior_rows_7d) OVER w_all_by_prior_rows_7d_vs_row AS prior_rows_7d_change,
        FIRST_VALUE(row_count) IGNORE NULLS OVER (ORDER BY prior_rows_7d_vs_row_count_distinct_ids_avg, prior_rows_7d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_count
    FROM sequenced
    WINDOW w_all_by_prior_rows_7d_vs_row AS (ORDER BY prior_rows_7d_vs_row_count_distinct_ids_avg, prior_rows_7d)
)
SELECT prior_rows_7d, measure, metric_value
FROM (
    SELECT prior_rows_7d, row_count_cume_dist, prior_rows_7d_vs_row_count_distinct_ids_avg
    FROM windowed
) src
UNPIVOT (
    metric_value FOR measure IN (row_count_cume_dist AS row_count_cume_dist_v, prior_rows_7d_vs_row_count_distinct_ids_avg AS m_prior_rows_7d_vs_row_count_distinct_ids_avg)
) unp

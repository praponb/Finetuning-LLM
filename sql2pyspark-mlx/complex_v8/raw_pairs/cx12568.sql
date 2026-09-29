WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.qty) AS prior_qty_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(prior_rows_1d) OVER (ORDER BY prior_rows_1d ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prior_rows_1d,
        ROUND(AVG(prior_qty_1d) OVER (PARTITION BY MONTH(moved_at) ORDER BY prior_qty_1d, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS prior_qty_1d_avg_2_prior,
        FIRST_VALUE(prior_rows_1d) OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY prior_qty_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_rows_1d,
        LEAD(moved_at) OVER (PARTITION BY MONTH(moved_at) ORDER BY prior_qty_1d, id) AS next_moved_at,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY YEAR(moved_at))) AS distinct_sku_id_in_moved_at_year,
        LAST_VALUE(moved_at) OVER (PARTITION BY sku_id, YEAR(moved_at) ORDER BY prior_qty_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at
    FROM joined_ranges
)
SELECT id, field, value
FROM (
    SELECT id, distinct_sku_id_in_moved_at_year, prior_qty_1d
    FROM with_trends
) src
UNPIVOT (
    value FOR field IN (distinct_sku_id_in_moved_at_year AS distinct_sku_id_in_moved_at_year_v, prior_qty_1d AS prior_qty_1d_v)
) unp
ORDER BY id, field

WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.unit_price) AS prior_unit_price_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
latest AS (
    -- Keep the earliest row per sku_id
    SELECT
        id,
        sku_id,
        moved_at,
        prior_rows_1d,
        prior_unit_price_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY sku_id ORDER BY prior_unit_price_1d, id) AS row_num
        FROM covered
    ) ranked
    WHERE row_num = 1
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        sku_id,
        PERCENTILE_APPROX(prior_unit_price_1d, 0.5) AS median_prior_unit_price_1d,
        COUNT(*) AS cnt,
        MAX(moved_at) AS last_moved_at,
        MIN(prior_rows_1d) AS min_prior_rows_1d,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(prior_rows_1d), 2) AS stddev_prior_rows_1d
    FROM latest
    GROUP BY sku_id
)
SELECT
    sku_id,
    cnt,
    min_prior_rows_1d,
    MAX(last_moved_at) AS last_last_moved_at
FROM rollup_base
GROUP BY GROUPING SETS ((sku_id, cnt, min_prior_rows_1d), (sku_id), ())
ORDER BY sku_id NULLS LAST, cnt NULLS LAST, min_prior_rows_1d NULLS LAST

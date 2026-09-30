WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE opened_date > DATE '2023-04-28'
      AND LENGTH(store_name) > 7
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date_plus_months
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_date
    FROM kept s
    JOIN LATERAL (
            SELECT c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Window metrics over several different partitions
SELECT
    *,
    LAST_VALUE(region, TRUE) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
    NTILE(5) OVER (PARTITION BY city, region ORDER BY sqft DESC, id ASC) AS sqft_bucket,
    SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft
FROM with_latest;

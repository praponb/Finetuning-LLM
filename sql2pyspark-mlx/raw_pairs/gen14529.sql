WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(city, 1, 2) AS city_prefix
    FROM stores
    WHERE region IS DISTINCT FROM 'East'
      AND region = 'South'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.shipping_fee
    FROM valid s
    JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_stores AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.sale_date
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (ORDER BY opened_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        LAG(shipping_fee, 2) OVER (PARTITION BY region ORDER BY shipping_fee ASC, id ASC) AS prev_shipping_fee,
        NTILE(5) OVER (PARTITION BY YEAR(opened_date) ORDER BY shipping_fee ASC, id ASC) AS shipping_fee_bucket,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date), city ORDER BY shipping_fee DESC) AS shipping_fee_dense_rank
    FROM with_last_stores
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft,
    city_prefix,
    shipping_fee,
    sale_date,
    sqft_avg_6
FROM metrics;

WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        QUARTER(opened_date) AS opened_date_quarter,
        ROUND(sqft, 0) AS sqft_rounded
    FROM stores
    WHERE region IS NOT NULL
      AND city NOT IN ('Singapore', 'Tokyo')
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        x.amount,
        x.order_shipping_fee
    FROM base s
    JOIN LATERAL (
            SELECT c.amount, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        region,
        amount,
        NTILE(10) OVER (PARTITION BY region ORDER BY amount DESC, id) AS amount_bucket,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY region, city ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY order_shipping_fee DESC) AS order_shipping_fee_dense_rank
    FROM latest_match
    WHERE store_name LIKE 'Air%'
),
latest_match_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.last_restock,
        latest.quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match_stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.last_restock
        )
)
SELECT
    id,
    region,
    amount,
    amount_bucket,
    first_known_amount,
    last_restock,
    quantity
FROM eligible
ORDER BY first_known_amount;

WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY DATE_TRUNC('week', opened_date), city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        SUM(sqft) OVER (PARTITION BY region) AS region_sum_sqft
    FROM stores
    WHERE store_name LIKE 'R%'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.order_amount,
        x.shipping_fee
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.amount AS order_amount, c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(order_amount) OVER (PARTITION BY region ORDER BY cumulative_sqft, id) AS prev_order_amount,
        LEAD(order_amount) OVER (PARTITION BY region ORDER BY cumulative_sqft, id) AS next_order_amount,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY cumulative_sqft, id) AS next_sqft,
        LAG(sqft) OVER (PARTITION BY region ORDER BY cumulative_sqft, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY cumulative_sqft, id) AS seq_no
    FROM with_last
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date, id) AS rn
        FROM scored_stores
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft,
    cumulative_sqft,
    region_sum_sqft,
    shipping_fee,
    seq_no
FROM deduped

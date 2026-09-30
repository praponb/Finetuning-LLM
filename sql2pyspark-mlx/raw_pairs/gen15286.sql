WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock < s.opened_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.order_shipping_fee,
        latest.amount
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee, c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS LAST, id) AS latest_rank
        FROM with_latest
    ) x
    WHERE latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        region,
        amount,
        MAX(order_shipping_fee) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_order_shipping_fee,
        COUNT(order_shipping_fee) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_order_shipping_fee,
        SUM(order_shipping_fee) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_order_shipping_fee,
        LEAD(order_shipping_fee, 1) OVER (PARTITION BY city ORDER BY order_shipping_fee, id) AS next_order_shipping_fee
    FROM deduped
    WHERE store_name IS NOT NULL
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS amount_per_london,
        ROUND(AVG(running_count_order_shipping_fee), 2) AS avg_running_count_order_shipping_fee,
        MIN(amount) AS min_amount,
        SUM(next_order_shipping_fee) AS total_next_order_shipping_fee
    FROM metrics
    GROUP BY region
)
SELECT
    region,
    amount_per_london,
    avg_running_count_order_shipping_fee,
    total_next_order_shipping_fee,
    CASE WHEN region RLIKE '^(North|South)$' THEN 'EXTENDED' WHEN region RLIKE '^(East|West)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS region_zone
FROM grouped
ORDER BY avg_running_count_order_shipping_fee;

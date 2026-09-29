WITH cleaned AS (
    SELECT *, COALESCE(cost, 0) AS cost_filled
    FROM products
    WHERE launch_date >= '2024-03-01'
),
enriched AS (
    SELECT
        src.*,
        d.rating,
        d.supplier_name,
        d.country
    FROM cleaned src
    INNER JOIN suppliers d
        ON src.supplier_id = d.id
),
latest_match AS (
    SELECT
        s.*,
        top1.unit_price,
        top1.order_item_quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    SELECT s.*, latest.revenue
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY price DESC, id ASC) AS rank_in_group
        FROM with_last
    ) t
    WHERE rank_in_group = 1
),
scored AS (
    SELECT
        *,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY category ORDER BY unit_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        SUM(rating) OVER (PARTITION BY country ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY supplier_id), 4) AS revenue_share,
        AVG(rating) OVER (PARTITION BY category ORDER BY cost ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5,
        AVG(revenue) OVER (ORDER BY price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_avg_6
    FROM latest
)
SELECT
    s.id,
    s.category,
    s.price,
    s.launch_date,
    s.supplier_id,
    s.supplier_name,
    s.country,
    s.order_item_quantity,
    s.revenue,
    (SELECT SUM(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS sum_product_price
FROM scored s
ORDER BY supplier_id DESC NULLS LAST

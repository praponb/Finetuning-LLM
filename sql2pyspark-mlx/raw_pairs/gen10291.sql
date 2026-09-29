WITH joined AS (
    SELECT
        e.*,
        d.product_name,
        d.brand,
        d.category
    FROM store_sales e
    LEFT JOIN products d
        ON e.product_id = d.id
),
valid AS (
    SELECT *, LOWER(category) AS category_lower
    FROM joined
    WHERE sale_date >= TO_DATE('2023-05-16')
),
with_dims AS (
    SELECT e.*, d.opened_date
    FROM valid e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY brand ORDER BY sale_date DESC) AS rank_in_group
    FROM with_dims
    QUALIFY rank_in_group = 1
),
deduped AS (
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY units DESC, id) = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.product_name,
    s.brand,
    s.category,
    s.category_lower,
    (SELECT SUM(t2.quantity) FROM order_items t2 WHERE t2.product_id = s.product_id) AS sum_order_item_quantity
FROM deduped s
ORDER BY store_id DESC

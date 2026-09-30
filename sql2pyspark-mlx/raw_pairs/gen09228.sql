WITH cleaned AS (
    SELECT *, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM order_items
    WHERE unit_price > 150
),
cleaned_order_items AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        greatest_unit_price_quantity,
        ROUND(unit_price, 2) AS unit_price_rounded
    FROM cleaned
    WHERE product_id IN (19, 30)
),
joined AS (
    SELECT
        e.id,
        e.product_id,
        e.greatest_unit_price_quantity,
        d.cost,
        d.stock_qty,
        d.launch_date
    FROM cleaned_order_items e
    JOIN products d
        ON e.product_id <=> d.id
    WHERE d.price BETWEEN 820 AND 1140
),
latest AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY greatest_unit_price_quantity DESC NULLS LAST, id) = 1
),
with_dims AS (
    SELECT
        src.id,
        src.product_id,
        src.cost,
        src.stock_qty,
        p.brand,
        p.stock_qty AS product_stock_qty
    FROM latest src
    JOIN products p
        ON src.product_id <=> p.id
)
SELECT
    d.category,
    a.brand AS order_item_brand,
    COUNT(*) AS num_rows,
    MAX(a.product_stock_qty) AS max_product_stock_qty,
    GROUPING_ID() AS level_id
FROM with_dims a
JOIN products d ON a.product_id = d.id
GROUP BY GROUPING SETS ((d.category, a.brand), (d.category), ())
ORDER BY max_product_stock_qty;

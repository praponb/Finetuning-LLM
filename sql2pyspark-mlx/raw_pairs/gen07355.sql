SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST, id) AS rn
FROM order_items
QUALIFY rn = 1

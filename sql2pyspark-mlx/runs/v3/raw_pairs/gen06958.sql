SELECT
    id,
    cost,
    launch_date,
    brand,
    COALESCE(stock_qty, 0) AS stock_qty_filled,
    CONCAT_WS(',', category, brand) AS category_brand_key
FROM products;

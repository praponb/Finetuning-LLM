SELECT
    ss.id,
    ss.sale_date,
    ss.product_id,
    ss.revenue
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE category IN ('Fashion', 'Sports', 'Grocery'));

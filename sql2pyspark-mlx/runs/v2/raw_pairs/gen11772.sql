SELECT
    ss.sale_date,
    ss.store_id,
    ss.product_id,
    s.opened_date
FROM store_sales ss
FULL OUTER JOIN stores s ON s.id = ss.store_id
ORDER BY ss.store_id ASC, product_id DESC

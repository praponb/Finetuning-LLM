SELECT
    ss.id,
    ss.store_id,
    p.stock_qty,
    p.category,
    s.city,
    s.opened_date,
    TRIM(s.city) AS clean_city
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
FULL OUTER JOIN stores s ON s.id = ss.store_id
WHERE p.price < 350 OR ss.product_id IN (1, 5, 7)
ORDER BY id, 5 DESC;

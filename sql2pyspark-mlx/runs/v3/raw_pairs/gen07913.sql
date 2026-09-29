SELECT id, product_id
FROM store_sales
WHERE sale_date > '2023-03-02'
ORDER BY product_id DESC, 1 DESC;

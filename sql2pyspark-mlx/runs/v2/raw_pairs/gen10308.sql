SELECT s.id, s.opened_date, t.revenue_count
FROM stores s
LEFT JOIN (SELECT store_id, COUNT(revenue) AS revenue_count
    FROM store_sales
    WHERE product_id IN (3, 8, 10)
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.revenue_count > 8

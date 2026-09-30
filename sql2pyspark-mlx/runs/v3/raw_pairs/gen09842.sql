SELECT id, category, price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) pos
    FROM products
    WHERE launch_date < '2025-02-02'
) ranked
WHERE pos <= 5;

SELECT *
FROM (
    SELECT supplier_id, SUM(CASE WHEN category = 'Electronics' THEN price ELSE 0 END) AS electronics_price, MAX(launch_date) AS latest_launch_date
    FROM products
    GROUP BY supplier_id
) AS s
WHERE electronics_price > 40;

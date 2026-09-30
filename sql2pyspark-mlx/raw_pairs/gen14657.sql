SELECT
    brand,
    QUARTER(launch_date) launch_quarter,
    supplier_id,
    MIN(price) min_price
FROM products
GROUP BY 1, 2, 3
HAVING MIN(price) < 900;

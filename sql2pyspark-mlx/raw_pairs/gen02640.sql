SELECT
    *,
    NTILE(4) OVER (PARTITION BY MONTH(launch_date), category ORDER BY cost DESC, id) AS cost_bucket,
    LAG(price, 2) OVER (PARTITION BY category ORDER BY launch_date, id) AS prev_price
FROM products;

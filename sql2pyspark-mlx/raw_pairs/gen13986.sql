WITH ranked AS (SELECT id, city, opened_date, sqft, RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) AS rnk FROM stores) SELECT id, city, opened_date, sqft FROM ranked WHERE rnk <= 3;

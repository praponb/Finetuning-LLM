SELECT id, city, sqft, opened_date, LAST_VALUE(opened_date) OVER (PARTITION BY city ORDER BY sqft DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_date FROM stores

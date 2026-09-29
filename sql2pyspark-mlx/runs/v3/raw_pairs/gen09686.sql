SELECT id, city, sqft, DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft) AS dense_rnk FROM stores ORDER BY sqft;

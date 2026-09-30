WITH ranked_stores AS (SELECT *, RANK() OVER (PARTITION BY region ORDER BY sqft) AS pos FROM stores WHERE region <> 'East') SELECT * FROM ranked_stores WHERE pos <= 3 ORDER BY city;

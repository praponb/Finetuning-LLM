WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS rnk FROM stores) SELECT id, city, sqft FROM numbered WHERE rnk <= 3

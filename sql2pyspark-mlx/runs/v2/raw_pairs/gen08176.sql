WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rnk FROM stores) SELECT id, region, sqft FROM ranked WHERE rnk <= 2;

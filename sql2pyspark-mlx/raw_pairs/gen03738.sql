WITH ranked_stores AS (SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date) AS pos FROM stores WHERE region = 'North') SELECT * FROM ranked_stores WHERE pos = 1;

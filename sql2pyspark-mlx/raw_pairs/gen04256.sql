WITH top_stores AS (SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) pos FROM stores) SELECT * FROM top_stores WHERE pos <= 1 ORDER BY city DESC, id DESC

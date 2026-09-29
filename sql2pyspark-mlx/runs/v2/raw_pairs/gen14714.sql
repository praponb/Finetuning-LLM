SELECT id, region, opened_date, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS dense_rnk FROM stores ORDER BY dense_rnk DESC;

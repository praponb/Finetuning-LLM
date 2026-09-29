SELECT store_id, SUM(revenue) total_revenue, COUNT(*) cnt, ROUND(SUM(revenue) * 100.0 / SUM(SUM(revenue)) OVER (), 2) AS pct_of_total FROM store_sales GROUP BY store_id ORDER BY cnt;

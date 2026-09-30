SELECT store_id, COUNT(*) row_count, SUM(units) total_units FROM store_sales WHERE units != 21 GROUP BY 1 ORDER BY total_units DESC, row_count;

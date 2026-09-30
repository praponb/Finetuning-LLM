SELECT id, revenue, units, sale_date FROM store_sales WHERE revenue > (SELECT AVG(revenue) FROM store_sales WHERE units BETWEEN 13 AND 15) ORDER BY id DESC, units DESC

SELECT sale_date FROM store_sales WHERE sale_date > '2025-07-01' INTERSECT SELECT sale_date FROM store_sales WHERE units BETWEEN 12 AND 25 ORDER BY sale_date DESC;

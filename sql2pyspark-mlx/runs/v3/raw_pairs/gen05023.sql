SELECT sale_date FROM store_sales WHERE units >= 22 INTERSECT SELECT sale_date FROM store_sales WHERE sale_date > '2023-04-01';

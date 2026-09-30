SELECT store_id FROM store_sales INTERSECT ALL SELECT store_id FROM store_sales WHERE sale_date >= '2024-07-02';

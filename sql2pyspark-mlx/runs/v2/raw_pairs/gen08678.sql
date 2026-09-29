SELECT products.brand, COUNT(*) AS cnt FROM store_sales INNER JOIN products ON store_sales.product_id = products.id GROUP BY products.brand;

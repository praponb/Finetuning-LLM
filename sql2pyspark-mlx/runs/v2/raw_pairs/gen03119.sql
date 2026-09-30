SELECT store_sales.sale_date, stores.city, products.stock_qty
FROM store_sales
INNER JOIN stores ON store_sales.store_id = stores.id
RIGHT JOIN products ON store_sales.product_id = products.id;

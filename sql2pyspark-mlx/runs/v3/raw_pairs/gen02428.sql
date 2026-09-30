SELECT p.brand, p.price, p.stock_qty, ss.product_id, ss.revenue, ss.units FROM products p LEFT JOIN store_sales ss ON p.id = ss.product_id WHERE ss.sale_date >= '2023-10-13';

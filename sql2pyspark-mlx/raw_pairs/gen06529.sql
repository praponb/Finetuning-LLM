SELECT revenue FROM store_sales WHERE product_id = 2 INTERSECT SELECT cost AS revenue FROM products WHERE stock_qty < 342;

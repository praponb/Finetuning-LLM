select products.id, products.brand, products.stock_qty, products.category from products where products.id in (select product_id from order_items where order_id in (1, 7, 14, 19));

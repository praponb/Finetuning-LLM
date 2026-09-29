SELECT id, brand, price, supplier_id, stock_qty + price stock_qty_plus_price, LAST_DAY(launch_date) launch_date_month_end FROM products WHERE product_name NOT LIKE 'Wa%';

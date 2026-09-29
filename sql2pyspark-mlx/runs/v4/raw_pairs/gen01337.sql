select cost, supplier_id, launch_date, least(price, stock_qty) as least_price_stock_qty from products where category = 'Grocery' and cost <= 410;

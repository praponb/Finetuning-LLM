select *, stock_qty * cost as stock_qty_times_cost, coalesce(product_name, 'N/A') as product_name_filled from products where category <> 'Grocery';

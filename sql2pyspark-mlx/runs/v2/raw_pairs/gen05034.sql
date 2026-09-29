select id, stock_qty, supplier_id, tags, price + cost as margin, floor(cost) as cost_whole from products;

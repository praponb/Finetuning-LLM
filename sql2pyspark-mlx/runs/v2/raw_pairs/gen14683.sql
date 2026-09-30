select p.id, p.launch_date, p.stock_qty, s.country, s.supplier_name from products p full outer join suppliers s on p.supplier_id = s.id where p.product_name is not null order by p.stock_qty;

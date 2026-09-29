select p.id, p.product_name, p.stock_qty, s.supplier_name from products p inner join suppliers s on p.supplier_id = s.id

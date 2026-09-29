select p.supplier_id, p.stock_qty, ss.units, ss.product_id from products p left join store_sales ss on p.id = ss.product_id where p.price < 610

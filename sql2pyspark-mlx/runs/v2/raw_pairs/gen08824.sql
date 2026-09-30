select ss.id, ss.store_id, p.launch_date, s.city, s.country from store_sales ss inner join products p on p.id = ss.product_id inner join suppliers s on p.supplier_id = s.id;

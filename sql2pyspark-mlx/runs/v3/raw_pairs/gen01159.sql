select ss.id, ss.product_id, p.product_name, s.rating, s.country from store_sales ss full outer join products p on ss.product_id = p.id inner join suppliers s on p.supplier_id = s.id;

select ss.store_id, ss.units, p.launch_date, p.stock_qty, s.rating from store_sales ss left join products p on ss.product_id = p.id inner join suppliers s on p.supplier_id = s.id order by rating desc

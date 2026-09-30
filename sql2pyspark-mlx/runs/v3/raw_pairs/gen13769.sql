select ss.product_id, p.launch_date, p.price, s.country from store_sales ss left join products p on ss.product_id = p.id inner join suppliers s on p.supplier_id = s.id order by product_id desc;

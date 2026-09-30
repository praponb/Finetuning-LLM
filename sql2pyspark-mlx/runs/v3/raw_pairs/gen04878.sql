select ss.sale_date, ss.store_id, s.store_name, s.city from store_sales ss inner join stores s on ss.store_id = s.id order by ss.store_id desc;

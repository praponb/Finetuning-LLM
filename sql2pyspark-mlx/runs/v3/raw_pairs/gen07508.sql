select ss.sale_date, s.store_name, s.city from store_sales ss inner join stores s on ss.store_id = s.id where upper(s.store_name) like '%LD %'

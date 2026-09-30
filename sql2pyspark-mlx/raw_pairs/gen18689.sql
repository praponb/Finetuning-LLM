select ss.id, ss.sale_date, ss.units, s.region from store_sales ss inner join stores s on ss.store_id = s.id;

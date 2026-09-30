select ss.units, s.sqft, s.city from store_sales ss inner join stores s on ss.store_id = s.id where not (s.sqft between 7338 and 9191 and s.opened_date <= '2023-10-01') order by units;

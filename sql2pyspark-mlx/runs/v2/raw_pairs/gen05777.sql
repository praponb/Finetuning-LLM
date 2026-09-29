select i.store_id, s.opened_date, s.store_name from inventory i left join stores s on i.store_id = s.id where s.region not in ('Central', 'West') and i.store_id in (1, 3, 4)

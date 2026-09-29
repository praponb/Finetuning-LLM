select s.city, min(i.last_restock) min_last_restock, min(i.last_restock) first_last_restock from inventory i inner join stores s on s.id = i.store_id where year(s.opened_date) >= 2023 group by s.city

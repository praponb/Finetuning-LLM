select month(s.opened_date) opened_month, count(i.quantity) quantity_count from inventory i left join stores s on s.id = i.store_id group by month(s.opened_date)

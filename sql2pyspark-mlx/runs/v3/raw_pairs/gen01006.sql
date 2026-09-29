select quarter(s.opened_date) as opened_quarter, max(i.quantity) as max_quantity from inventory i full outer join stores s on i.store_id = s.id group by quarter(s.opened_date);

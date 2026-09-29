select i.id, i.quantity, s.region, s.opened_date, p.stock_qty from inventory i left outer join stores s on i.store_id = s.id full outer join products p on p.id = i.product_id order by i.quantity;

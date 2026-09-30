select stores.region, stores.opened_date, orders.order_date, orders.discount, orders.shipping_fee from stores left join orders on stores.id = orders.store_id;

select o.id, o.discount, o.customer_id, o.shipping_fee, s.opened_date, floor(o.amount) as amount_whole from orders o left join stores s on o.store_id = s.id where o.shipping_fee between 18 and 24;

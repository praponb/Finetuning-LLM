select s.city, s.sqft, s.store_name, o.status, o.discount * o.shipping_fee as discount_times_shipping_fee from stores s left join orders o on s.id = o.store_id;

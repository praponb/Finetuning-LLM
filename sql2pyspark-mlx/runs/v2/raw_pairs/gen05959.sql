SELECT s.region, s.opened_date, s.sqft, o.shipping_fee, o.status, o.customer_id FROM stores s LEFT OUTER JOIN orders o ON o.store_id = s.id;

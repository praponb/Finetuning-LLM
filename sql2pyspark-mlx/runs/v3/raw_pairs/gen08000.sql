SELECT store_id, MAX(shipping_fee) AS max_shipping_fee FROM orders GROUP BY store_id ORDER BY store_id DESC, max_shipping_fee DESC;

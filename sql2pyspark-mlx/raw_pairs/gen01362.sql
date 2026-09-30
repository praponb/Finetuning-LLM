SELECT store_id, discount, status, LAST_DAY(order_date) AS order_date_month_end FROM orders WHERE shipping_fee < 21;

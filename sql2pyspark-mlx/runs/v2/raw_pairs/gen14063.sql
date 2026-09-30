SELECT id, shipping_fee, DATE_ADD(order_date, 62) AS order_date_plus_62d FROM orders;

SELECT discount, LAST_DAY(order_date) AS order_date_month_end FROM orders WHERE channel <> 'partner' AND (amount < 660 OR shipping_fee < 25) ORDER BY discount DESC;

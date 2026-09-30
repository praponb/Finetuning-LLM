SELECT *, amount / NULLIF(shipping_fee, 0) AS amount_per_shipping_fee FROM orders ORDER BY 1 ASC;

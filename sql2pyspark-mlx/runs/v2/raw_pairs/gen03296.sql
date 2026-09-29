SELECT * FROM orders WHERE channel IN ('store', 'partner', 'mobile') ORDER BY shipping_fee DESC;

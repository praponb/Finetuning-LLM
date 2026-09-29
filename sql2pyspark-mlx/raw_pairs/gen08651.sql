SELECT
    channel,
    MAX(amount) FILTER (WHERE channel = 'partner') AS partner_max_amount,
    SUM(CASE WHEN channel = 'web' THEN shipping_fee ELSE 0 END) AS web_shipping_fee,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    CORR(discount, shipping_fee) AS corr_discount_shipping_fee,
    SUM(CASE WHEN status = 'PENDING' THEN shipping_fee ELSE 0 END) AS pending_shipping_fee
FROM orders
GROUP BY channel
HAVING pending_shipping_fee > 0;

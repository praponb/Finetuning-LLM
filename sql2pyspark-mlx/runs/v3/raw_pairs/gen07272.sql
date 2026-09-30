SELECT
    channel,
    MAX(discount) AS max_discount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee
FROM orders
WHERE status = 'SHIPPED'
GROUP BY channel

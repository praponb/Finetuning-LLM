SELECT
    status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(shipping_fee) AS shipping_fee_count,
    ROUND(AVG(discount), 2) AS avg_discount
FROM orders
WHERE customer_id IN (10, 29, 33, 35)
GROUP BY status
ORDER BY status ASC, shipping_fee_count DESC

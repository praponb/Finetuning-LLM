SELECT status, discount_range, status_list
FROM (
    SELECT status, MAX(discount) - MIN(discount) AS discount_range, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM orders
    GROUP BY status
) AS sub
WHERE discount_range >= 22

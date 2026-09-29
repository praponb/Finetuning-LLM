WITH OrdersByCustomer AS (
    SELECT customer_id, COUNT(DISTINCT discount) AS unique_channels, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM orders
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    c.segment,
    c.country,
    O.unique_channels,
    O.status_values
FROM customers c
INNER JOIN OrdersByCustomer O ON c.id = O.customer_id;

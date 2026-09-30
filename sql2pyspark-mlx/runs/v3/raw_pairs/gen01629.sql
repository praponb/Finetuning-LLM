SELECT
    s.opened_date,
    s.city,
    s.sqft,
    i.store_id,
    i.quantity
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
WHERE s.opened_date BETWEEN '2024-06-01' AND '2024-09-03' OR s.city NOT IN ('Berlin', 'New York', 'Bangkok')
ORDER BY i.quantity DESC

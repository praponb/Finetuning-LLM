SELECT
    c.id,
    c.age,
    c.name,
    c.is_active
FROM customers c
WHERE c.id IN (SELECT customer_id FROM sessions)
ORDER BY 3, c.id DESC

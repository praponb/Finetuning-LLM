WITH valid AS (
    SELECT *, CONCAT_WS('-', country, device) AS country_device_key
    FROM sessions
    WHERE started_at > TIMESTAMP '2024-02-21 09:00:00'
)
SELECT country, customer_id
FROM valid
INTERSECT ALL
SELECT country, customer_id
FROM sessions
WHERE customer_id IN (1, 34);

SELECT amount AS value
FROM transactions
UNION
SELECT cost AS value
FROM shipments
ORDER BY value;

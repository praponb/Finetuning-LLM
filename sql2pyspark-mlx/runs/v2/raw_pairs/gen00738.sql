SELECT
    s.sqft,
    s.city,
    o.order_date,
    CAST(o.amount AS INT) AS amount_int
FROM stores s
INNER JOIN orders o ON o.store_id = s.id;

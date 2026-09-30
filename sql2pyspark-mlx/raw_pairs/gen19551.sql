SELECT
    s.id,
    s.sqft,
    x.store_sale_sale_date
FROM stores s
JOIN LATERAL (
        SELECT c.sale_date AS store_sale_sale_date
        FROM store_sales c
        WHERE c.store_id = s.id
        ORDER BY c.units DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;

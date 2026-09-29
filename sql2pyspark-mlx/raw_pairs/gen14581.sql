SELECT
    s.*,
    latest.store_sale_units,
    latest.store_sale_sale_date
FROM stores s
LEFT JOIN LATERAL (
        SELECT c.units AS store_sale_units, c.sale_date AS store_sale_sale_date
        FROM store_sales c
        WHERE c.store_id = s.id
          AND c.sale_date <= s.opened_date
        ORDER BY c.sale_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;

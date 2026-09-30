WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS latest_rank
    FROM shipments
    QUALIFY latest_rank = 1
)
SELECT
    id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    latest_rank,
    CASE WHEN carrier RLIKE '^(UPS|Kerry)$' THEN 'CORE' ELSE 'EXTENDED' END AS carrier_zone
FROM ranked
ORDER BY carrier_zone DESC NULLS FIRST, id DESC;

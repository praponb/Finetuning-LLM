WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        CONCAT(status, ' ', carrier) AS status_carrier,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-06-24')
      AND carrier IS NOT DISTINCT FROM 'Flash'
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY shipped_date DESC, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) AS row_seq
FROM base
WHERE YEAR(delivered_date) >= 2025;

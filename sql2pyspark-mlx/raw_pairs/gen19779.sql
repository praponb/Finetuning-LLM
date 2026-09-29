WITH cleaned AS (
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        TRIM(carrier) AS clean_carrier
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-06-04')
      AND shipped_date < '2023-07-01'
)
SELECT id, carrier
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY cost DESC, id) AS latest_rank
    FROM cleaned
) t
WHERE latest_rank = 1;

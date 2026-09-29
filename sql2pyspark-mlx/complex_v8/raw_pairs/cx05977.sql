WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE status NOT IN ('EXPIRED', 'CANCELLED')
      AND end_date BETWEEN DATE '2025-06-08' AND DATE '2026-11-25'
)
SELECT holder_id, premium AS amount, start_date
FROM scoped
WHERE status = 'LAPSED'
UNION ALL
SELECT id AS holder_id, risk_score AS amount, joined_on AS start_date
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'G%'
UNION ALL
SELECT holder_id, premium AS amount, start_date
FROM policies
ORDER BY start_date DESC, holder_id

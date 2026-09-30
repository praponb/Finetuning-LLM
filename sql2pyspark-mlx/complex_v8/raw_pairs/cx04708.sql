WITH base AS (
    -- Scope the policyholders rows
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        DATE_FORMAT(joined_on, 'yyyy-MM-dd') AS joined_on_label
    FROM policyholders
    WHERE (region IN ('Central', 'East') OR UPPER(TRIM(holder_name)) LIKE 'L%')
),
all_events AS (
    -- Stack the branches
    SELECT segment AS label, risk_score AS amount
    FROM base
    WHERE joined_on BETWEEN DATE '2024-05-23' AND DATE '2025-05-01'
    UNION ALL
    SELECT product, premium
    FROM policies
)
SELECT label, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM all_events
GROUP BY label
ORDER BY label

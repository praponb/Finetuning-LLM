WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        COALESCE(premium, 0.0) AS premium_filled_2,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM recent
    WHERE premium_filled > (SELECT MIN(premium_filled) FROM recent)
),
screened_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IS NOT NULL
              AND c.claim_date < DATE '2025-03-31'
        )
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM screened_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status <> 'REJECTED'
              AND c.approved_amount > 2887.3
        )
)
SELECT
    status,
    holder_id,
    COUNT(*) AS n_rows
FROM qualified
GROUP BY GROUPING SETS ((status, holder_id), (status), (holder_id), ())
ORDER BY status NULLS LAST, holder_id NULLS LAST

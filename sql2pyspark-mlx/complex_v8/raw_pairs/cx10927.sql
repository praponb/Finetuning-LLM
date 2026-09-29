WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE status NOT IN ('CANCELLED', 'LAPSED')
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM base s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.product = 'TRAVEL'
              AND p.end_date BETWEEN DATE '2025-10-12' AND DATE '2027-01-03'
        )
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id >= 3
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(product)) AS product_len,
        CONCAT_WS('-', product, status) AS product_status_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM qualified
),
qualified_2 AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM annotated s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
      AND s.holder_id <= 7
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.product_len,
        s.product_status_label,
        s.status_or_unknown,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM qualified_2 s
    JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'G%'
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    status,
    product_len,
    product_status_label,
    status_or_unknown,
    holder_segment,
    holder_joined_on,
    holder_risk_score
FROM enriched_holder
WHERE end_date BETWEEN DATE '2025-10-12' AND DATE '2026-12-05'
ORDER BY holder_segment

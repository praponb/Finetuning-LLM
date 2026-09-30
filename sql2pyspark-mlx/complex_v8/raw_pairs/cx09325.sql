WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE (status = 'LAPSED' OR end_date BETWEEN DATE '2025-04-17' AND DATE '2027-03-20')
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.product_prefix,
        s.status_or_unknown,
        p.risk_score AS holder_risk_score
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment = 'RETAIL'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(holder_risk_score) OVER (PARTITION BY status_or_unknown ORDER BY start_date DESC, id) AS next_holder_risk_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_or_unknown ORDER BY start_date DESC), 3) AS start_date_percent_rank
    FROM enriched
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date IS NOT NULL
              AND p.start_date >= DATE '2024-10-22'
        )
)
SELECT
    holder_id,
    COALESCE(status_or_unknown, 'ALL') AS status_or_unknown,
    COALESCE(status, 'ALL') AS status,
    GROUPING(holder_id) AS is_total_holder_id,
    COUNT(*) AS cnt,
    ROUND(SUM(holder_risk_score), 2) AS total_holder_risk_score,
    MAX(start_date) AS last_start_date
FROM screened
GROUP BY ROLLUP(holder_id, status_or_unknown, status)
ORDER BY holder_id NULLS LAST, status_or_unknown NULLS LAST, status NULLS LAST

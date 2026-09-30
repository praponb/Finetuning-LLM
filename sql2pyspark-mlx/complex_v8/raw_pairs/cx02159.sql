WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE start_date >= DATE '2024-06-19'
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status_len,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    COALESCE(holder_region, 'ALL') AS holder_region,
    status_len,
    holder_id,
    COUNT(*) AS cnt
FROM combined_details
GROUP BY GROUPING SETS ((holder_region, status_len, holder_id), (holder_region), (status_len))

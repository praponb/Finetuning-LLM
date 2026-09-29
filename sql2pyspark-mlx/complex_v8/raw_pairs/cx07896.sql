WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE holder_id >= 14
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region,
        p.holder_name
    FROM recent_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    status,
    product,
    holder_id,
    ROUND(100 * COUNT(CASE WHEN holder_name IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_holder_name_is_not_null
FROM enriched
GROUP BY GROUPING SETS ((status, product, holder_id), (status), ())
ORDER BY status NULLS LAST, product NULLS LAST, holder_id NULLS LAST

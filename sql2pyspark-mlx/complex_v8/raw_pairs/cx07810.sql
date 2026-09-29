WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE premium BETWEEN 1541.8 AND 4417.0
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        p.joined_on AS holder_joined_on,
        p.holder_name,
        p.segment AS holder_segment
    FROM scoped s
    JOIN policyholders p ON p.id = s.holder_id
),
with_parent AS (
    -- Join policyholders
    SELECT s.*, p.region AS holder_region
    FROM with_details s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    holder_id,
    holder_region,
    product,
    GROUPING_ID() AS grouping_level,
    MIN(end_date) AS first_end_date,
    COUNT(*) FILTER (WHERE status IN ('EXPIRED', 'CANCELLED')) AS n_status_in_expired_cancelled
FROM with_parent
GROUP BY GROUPING SETS ((holder_id, holder_region, product), (holder_id), (holder_region), ())
ORDER BY holder_id NULLS LAST, holder_region NULLS LAST, product NULLS LAST

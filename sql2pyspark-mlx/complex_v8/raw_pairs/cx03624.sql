WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'HOME'
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date
    FROM cleaned_policys s
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'HEALTH'
        )
      AND s.status IS NOT NULL
)
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    COUNT(*) FILTER (WHERE status = 'ACTIVE') AS n_status_active
FROM with_activity
GROUP BY GROUPING SETS ((status, holder_id), (status), (holder_id), ())
ORDER BY status NULLS LAST, holder_id NULLS LAST

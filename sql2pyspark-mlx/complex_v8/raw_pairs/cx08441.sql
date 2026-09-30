WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE product IN ('LIFE', 'AUTO', 'HOME')
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_claimed_amount,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date
    FROM scoped_policys s
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        product,
        MAX(end_date) AS last_end_date,
        COUNT(*) FILTER (WHERE status NOT IN ('ACTIVE', 'EXPIRED')) AS n_status_not_in_active_expired
    FROM with_stats
    GROUP BY holder_id, product
)
-- Final projection
SELECT
    holder_id,
    product,
    last_end_date,
    n_status_not_in_active_expired,
    CASE WHEN product IN ('HEALTH', 'TRAVEL') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
    INITCAP(TRIM(product)) AS product_clean
FROM rollup_base
ORDER BY n_status_not_in_active_expired NULLS LAST, product

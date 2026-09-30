WITH recent_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
    FROM policyholders
    WHERE risk_score >= 0.1
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent_holders s
    WHERE s.id IN (
            SELECT p.holder_id
            FROM policies p
        )
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        NTH_VALUE(joined_on, 2) OVER (PARTITION BY region ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_joined_on,
        LAST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY region ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_joined_on,
        ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY joined_on), 3) AS joined_on_cume_dist,
        FIRST_VALUE(joined_on) OVER (PARTITION BY region ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on
    FROM screened
),
detail_rows_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.last_known_joined_on,
        s.first_joined_on,
        p.id AS policy_id,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM ranked s
    JOIN policies p ON p.holder_id = s.id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(approved_amount) AS min_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_approved_amount
    FROM detail_rows_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.reported_at < TIMESTAMP '2025-02-18 09:00:00'
        )
),
with_stats_2 AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.id) AS avg_policy_premium,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.id) AS last_policy_end_date,
        (SELECT MIN(p.premium) FROM policies p WHERE p.holder_id = s.id) AS min_policy_premium
    FROM filtered s
)
SELECT
    id,
    last_known_joined_on,
    first_joined_on,
    policy_id,
    policy_end_date,
    policy_product,
    avg_policy_premium,
    last_policy_end_date,
    min_policy_premium
FROM with_stats_2
ORDER BY policy_product, avg_policy_premium DESC

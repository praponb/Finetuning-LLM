WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown
    FROM policyholders
    WHERE risk_score > 0.1
),
scored AS (
    -- Derive helper columns
    SELECT *, DATE_ADD(joined_on, 30) AS joined_on_plus_30d
    FROM prepared
),
line_level AS (
    -- One row per policy
    SELECT
        s.id,
        s.segment,
        s.region_or_unknown,
        p.id AS policy_id,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM scored s
    LEFT JOIN policies p ON p.holder_id = s.id AND p.end_date IS NOT NULL
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product IS NOT NULL
        )
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM filtered s
    WHERE s.id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.status = 'LAPSED'
              AND p.product IN ('HOME', 'LIFE')
        )
),
exploded_detail_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.segment,
        s.region_or_unknown,
        s.policy_id,
        s.policy_end_date,
        s.policy_status,
        s.policy_start_date,
        p.id AS policy_id_2,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date_2
    FROM with_activity s
    JOIN policies p ON p.holder_id = s.id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM exploded_detail_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_end_date,
    policy_status,
    policy_start_date,
    policy_id_2,
    policy_premium,
    ROUND(policy_premium * 1.5, 2) AS policy_premium_adj
FROM summarized_rows

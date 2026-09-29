WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix
    FROM policies
    WHERE holder_id IS NOT NULL
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status_slug,
        s.status_prefix,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type
    FROM valid s
    JOIN claims c ON c.policy_id = s.id AND c.adjuster_id IS NOT NULL
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.claim_type <> 'theft' THEN 1 END) FROM claims c WHERE c.adjuster_id = s.claim_adjuster_id) AS claim_claim_type_count,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.claim_adjuster_id) AS max_claim_claimed_amount
    FROM line_level s
),
claim_adjuster_id_stats AS (
    -- Avg premium per claim_adjuster_id
    SELECT claim_adjuster_id, AVG(premium) AS avg_premium
    FROM summarized_rows
    GROUP BY claim_adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM summarized_rows s
    JOIN claim_adjuster_id_stats g ON g.claim_adjuster_id = s.claim_adjuster_id
    WHERE s.premium > g.avg_premium
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, claim_id, claim_type, claim_claim_type_count
    FROM outliers
) src
PIVOT (
    SUM(claim_claim_type_count)
    FOR claim_type IN ('theft', 'flood')
)
ORDER BY end_date, claim_id

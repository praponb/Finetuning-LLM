WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE claim_type IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount DESC, id) AS approved_amount_change,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount DESC), 3) AS claimed_amount_cume_dist
    FROM recent
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at,
        (SELECT MAX(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS max_claim_approved_amount
    FROM metrics s
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('-', status, claim_type_slug) AS status_claim_type_slug_label,
        GREATEST(approved_amount, approved_amount_change) AS greatest_approved_amount_approved_amount_change,
        DATE_FORMAT(last_claim_reported_at, 'yyyy-MM-dd') AS last_claim_reported_at_label
    FROM with_stats
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER w_adjuster_id_policy_id_by_max AS max_claim_approved_amount_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id, policy_id ORDER BY max_claim_approved_amount), 3) AS max_claim_approved_amount_cume_dist
    FROM tagged
    WINDOW w_adjuster_id_policy_id_by_max AS (PARTITION BY adjuster_id, policy_id ORDER BY max_claim_approved_amount, id)
),
status_stats AS (
    -- Max greatest_approved_amount_claimed_amount per status
    SELECT status, MAX(greatest_approved_amount_claimed_amount) AS max_greatest_approved_amount_claimed_amount
    FROM ranked
    GROUP BY status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_greatest_approved_amount_claimed_amount,
        ROUND(s.greatest_approved_amount_claimed_amount - g.max_greatest_approved_amount_claimed_amount, 2) AS greatest_approved_amount_claimed_amount_minus_ma
    FROM ranked s
    JOIN status_stats g ON g.status = s.status
    WHERE s.greatest_approved_amount_claimed_amount = g.max_greatest_approved_amount_claimed_amount
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    claim_type_slug,
    approved_amount_change,
    claimed_amount_cume_dist,
    max_claim_approved_amount,
    status_claim_type_slug_label,
    greatest_approved_amount_approved_amount_change,
    max_greatest_approved_amount_claimed_amount,
    greatest_approved_amount_claimed_amount_minus_ma
FROM compared
ORDER BY greatest_approved_amount_approved_amount_change DESC

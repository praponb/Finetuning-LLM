WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label_2,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE approved_amount > 2887.3
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.team AS adjuster_team,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id AND a.level BETWEEN 2 AND 3
    JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_status ORDER BY approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_policy_status, 0), 4) AS approved_amount_share_of_policy_status
    FROM enriched
    WINDOW w_policy_status AS (PARTITION BY policy_status)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref,
        LEAST(policy_premium, approved_amount) AS least_policy_premium_approved_amount
    FROM windowed
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE approved_amount_share_of_policy_status > (SELECT AVG(approved_amount_share_of_policy_status) FROM tagged)
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_id AS policy_holder_id
    FROM screened s
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    status,
    reported_at,
    reported_at_label,
    reported_at_label_2,
    status_slug,
    adjuster_team,
    policy_premium,
    policy_status,
    approved_amount_avg_2_prior,
    status_ref,
    least_policy_premium_approved_amount,
    policy_premium - approved_amount AS policy_premium_minus_approved_amount,
    FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
FROM joined_policy
ORDER BY status_slug DESC, claim_date DESC

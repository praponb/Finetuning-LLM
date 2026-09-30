WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 6 AND 10
      AND policy_id <= 23
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.status_prefix,
        a.level AS adjuster_level
    FROM prepared_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level > 2
),
adjuster_id_stats AS (
    -- Median claimed_amount per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM with_details_adjuster
    GROUP BY adjuster_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount,
        ROUND(s.claimed_amount - g.median_claimed_amount, 2) AS claimed_amount_minus_median
    FROM with_details_adjuster s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount >= g.median_claimed_amount
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        median_claimed_amount,
        FLOOR(median_claimed_amount / 10000) * 10000 AS median_claimed_amount_bucket,
        LOWER(TRIM(status_prefix)) AS status_prefix_clean,
        COALESCE(median_claimed_amount, 0.0) AS median_claimed_amount_filled
    FROM compared
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM annotated s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.status IS NOT NULL
        )
)
SELECT
    id,
    adjuster_id,
    claim_date,
    status,
    median_claimed_amount_bucket,
    status_prefix_clean,
    median_claimed_amount_filled
FROM screened
WHERE median_claimed_amount BETWEEN 2595.0 AND 45010.0
ORDER BY status, id NULLS LAST

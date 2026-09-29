WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN status IN ('REJECTED', 'OPEN') THEN 'group_a' WHEN status = 'IN_REVIEW' THEN 'group_b' ELSE 'other' END AS status_group,
        ABS(approved_amount - 9389.3) AS approved_amount_distance
    FROM claims
    WHERE status IN ('PAID', 'IN_REVIEW', 'OPEN')
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_date,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS max_claim_claimed_amount
    FROM base_claims s
),
policy_id_stats AS (
    -- Avg approved_amount_distance per policy_id
    SELECT policy_id, AVG(approved_amount_distance) AS avg_approved_amount_distance
    FROM profiled
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount_distance,
        ROUND(s.approved_amount_distance - g.avg_approved_amount_distance, 2) AS approved_amount_distance_minus_avg
    FROM profiled s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount_distance > g.avg_approved_amount_distance
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        TO_DATE(reported_at) AS reported_at_date,
        DATE_ADD(first_claim_date, 30) AS first_claim_date_plus_30d,
        LOWER(REGEXP_REPLACE(TRIM(status_group), ' ', '_')) AS status_group_slug
    FROM above_average
    WHERE avg_approved_amount_distance > 17500.0
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        approved_amount_distance,
        first_claim_date,
        approved_amount_distance_minus_avg,
        reported_at_date,
        first_claim_date_plus_30d,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY status, adjuster_id ORDER BY reported_at_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        FIRST_VALUE(max_claim_claimed_amount) IGNORE NULLS OVER (PARTITION BY status, adjuster_id ORDER BY reported_at_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_claim_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY MONTH(first_claim_date_plus_30d) ORDER BY approved_amount ASC, id) AS rn,
        MAX(claimed_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_max_claimed_amount,
        ROUND(AVG(max_claim_claimed_amount) OVER (ORDER BY approved_amount_distance_minus_avg DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS max_claim_claimed_amount_avg_2_prior
    FROM tagged
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claimed_amount,
    reported_at_date,
    second_claimed_amount,
    first_known_max_claim_claimed_amount,
    rn,
    status_policy_id_max_claimed_amount,
    max_claim_claimed_amount_avg_2_prior,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown,
    ABS(rn - 4) AS rn_distance
FROM sequenced
ORDER BY status_or_unknown

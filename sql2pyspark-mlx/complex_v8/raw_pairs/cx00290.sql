WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE status IN ('OPEN', 'PAID', 'APPROVED')
      AND approved_amount BETWEEN 1273.7 AND 23487.5
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, YEAR(reported_at))) AS distinct_claim_type_in_policy_id_reported_at_yea,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, YEAR(reported_at))) AS distinct_adjuster_id_in_policy_id_reported_at_ye,
        COUNT(*) OVER w_policy_id_reported_at_year AS rows_in_policy_id_reported_at_year
    FROM valid_claims
    WINDOW w_policy_id_reported_at_year AS (PARTITION BY policy_id, YEAR(reported_at))
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        distinct_claim_type_in_policy_id_reported_at_yea,
        distinct_adjuster_id_in_policy_id_reported_at_ye,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label
    FROM with_windows
    WHERE rows_in_policy_id_reported_at_year < 11
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        claim_date,
        claimed_amount,
        distinct_claim_type_in_policy_id_reported_at_yea,
        distinct_adjuster_id_in_policy_id_reported_at_ye,
        claimed_amount_filled,
        YEAR(claim_date) AS claim_date_year,
        MONTH(claim_date) AS claim_date_month
    FROM labeled
),
labeled_2 AS (
    -- Derive helper columns
    SELECT *, LAST_DAY(claim_date) AS claim_date_month_end
    FROM annotated
    WHERE claimed_amount >= 19337.6
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM labeled_2
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM labeled_2)
)
SELECT
    id,
    claimed_amount,
    distinct_claim_type_in_policy_id_reported_at_yea,
    distinct_adjuster_id_in_policy_id_reported_at_ye,
    claimed_amount_filled,
    claim_date_year
FROM screened
ORDER BY claimed_amount_filled, claim_date_year, id
LIMIT 5

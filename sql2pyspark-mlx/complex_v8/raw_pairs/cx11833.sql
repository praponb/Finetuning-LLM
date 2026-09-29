WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CASE WHEN claimed_amount >= 32878.5 THEN 'large' WHEN claimed_amount >= 18478.3 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        UPPER(TRIM(status)) AS status_clean,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM claims
    WHERE status IN ('OPEN', 'IN_REVIEW')
      AND adjuster_id IS NOT NULL
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE days_since_claim_date > (SELECT MIN(days_since_claim_date) FROM filtered)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        claimed_amount_tier,
        days_since_claim_date,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id)) AS distinct_claim_type_in_policy_id,
        NTILE(10) OVER (ORDER BY approved_amount, id) AS approved_amount_decile,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY approved_amount, id)) AS days_since_prev_claim_date,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_claimed_amount,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id
    FROM with_activity
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.days_since_claim_date,
        s.distinct_claim_type_in_policy_id,
        s.approved_amount_decile,
        s.rows_in_policy_id,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date
    FROM with_windows s
    JOIN policies p ON p.id = s.policy_id
),
policy_id_stats AS (
    -- Max rows_in_policy_id per policy_id
    SELECT policy_id, MAX(rows_in_policy_id) AS max_rows_in_policy_id
    FROM with_parent_policy
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_rows_in_policy_id,
        ROUND(s.rows_in_policy_id - g.max_rows_in_policy_id, 2) AS rows_in_policy_id_minus_max
    FROM with_parent_policy s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.rows_in_policy_id = g.max_rows_in_policy_id
)
SELECT
    approved_amount_decile,
    policy_id,
    distinct_claim_type_in_policy_id,
    GROUPING(approved_amount_decile) AS is_total_approved_amount_decile,
    MIN(reported_at) AS first_reported_at,
    ROUND(SUM(max_rows_in_policy_id) / NULLIF(SUM(days_since_claim_date), 0), 3) AS max_rows_in_policy_id_to_days_since_claim_date_r,
    ROUND(AVG(days_since_claim_date), 2) AS avg_days_since_claim_date
FROM vs_group
GROUP BY GROUPING SETS ((approved_amount_decile, policy_id, distinct_claim_type_in_policy_id), (approved_amount_decile), ())

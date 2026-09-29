WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE policy_id <= 3
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(reported_at_date) AS reported_at_date_year,
        CASE WHEN claimed_amount >= 33690.0 THEN 'large' WHEN claimed_amount >= 19347.2 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM valid
),
scored_2 AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM scored
    WHERE claimed_amount_per_approved_amount >= 2.0
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(approved_amount_filled) OVER (PARTITION BY reported_at_date_year) AS reported_at_date_year_max_approved_amount_filled,
        LEAD(reported_at) OVER (ORDER BY reported_at_date_year ASC, id) AS next_reported_at,
        LAST_VALUE(reported_at) OVER (ORDER BY reported_at_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at
    FROM scored_2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(claim_date, 3) OVER (PARTITION BY reported_at_date_year, policy_id ORDER BY next_reported_at, id) AS next3_claim_date,
        MAX(claimed_amount_filled) OVER (PARTITION BY status, policy_id) AS status_policy_id_max_claimed_amount_filled,
        claimed_amount - LAG(claimed_amount) OVER w_reported_at_date_year_policy AS claimed_amount_change
    FROM windowed
    WINDOW w_reported_at_date_year_policy AS (PARTITION BY reported_at_date_year, policy_id ORDER BY next_reported_at, id)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM with_windows
    WHERE reported_at_date_year > (SELECT MIN(reported_at_date_year) FROM with_windows)
)
SELECT id, field, value
FROM (
    SELECT id, claimed_amount_per_approved_amount, approved_amount
    FROM filtered
) src
UNPIVOT (
    value FOR field IN (claimed_amount_per_approved_amount AS m_claimed_amount_per_approved_amount, approved_amount AS m_approved_amount)
) unp

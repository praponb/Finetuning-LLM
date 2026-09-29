WITH scoped AS (
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
        ADD_MONTHS(claim_date, 1) AS claim_date_plus_1m,
        CASE WHEN status IN ('REJECTED', 'IN_REVIEW') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM claims
    WHERE (HOUR(reported_at) BETWEEN 9 AND 11 OR status = 'OPEN')
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT s.*, a.adjuster_name, a.level AS adjuster_level
    FROM scoped s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_date_plus_1m,
        status_group,
        claim_date_quarter,
        adjuster_level,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        LAG(reported_at) OVER w_claim_date_quarter_year_by_c AS prev_reported_at,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_level, policy_id ORDER BY claimed_amount DESC NULLS FIRST), 3) AS claimed_amount_cume_dist
    FROM with_details_adjuster
    WINDOW w_claim_date_quarter_year_by_c AS (PARTITION BY YEAR(claim_date_quarter) ORDER BY claim_date_quarter ASC, id)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.claim_date_plus_1m,
        s.status_group,
        s.claim_date_quarter,
        s.adjuster_level,
        a.adjuster_name,
        a.level AS adjuster_level_2,
        a.team AS adjuster_team
    FROM windowed s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level <= 3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_adjuster_level_2_claim_date AS rows_in_adjuster_level_2_claim_date_month
    FROM with_details
    WINDOW w_adjuster_level_2_claim_date AS (PARTITION BY adjuster_level_2, DATE_TRUNC('MONTH', claim_date))
),
latest AS (
    -- Keep the latest row per adjuster_team
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        claim_date_plus_1m,
        status_group,
        claim_date_quarter,
        adjuster_level,
        adjuster_name,
        adjuster_level_2,
        adjuster_team,
        rows_in_adjuster_level_2_claim_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team ORDER BY approved_amount DESC, id DESC) AS row_num
        FROM sequenced
    ) ranked
    WHERE row_num = 1
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.team AS adjuster_team_2,
        a.level AS adjuster_level_3,
        a.adjuster_name AS adjuster_adjuster_name,
        p.start_date AS policy_start_date
    FROM latest s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_date_plus_1m,
    status_group,
    claim_date_quarter,
    adjuster_level,
    adjuster_name,
    adjuster_level_2,
    rows_in_adjuster_level_2_claim_date_month,
    adjuster_team_2,
    adjuster_level_3,
    adjuster_adjuster_name,
    policy_start_date,
    ABS(adjuster_level_2 - 2) AS adjuster_level_2_distance,
    COALESCE(adjuster_level, 0) AS adjuster_level_filled
FROM enriched_adjuster
WHERE policy_id <= 4
ORDER BY rows_in_adjuster_level_2_claim_date_month NULLS LAST

WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        YEAR(reported_at) AS reported_at_year,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE policy_id <= 2
      AND adjuster_id >= 5
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_year,
        s.least_approved_amount_claimed_amount,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.premium AS policy_premium
    FROM base_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS max_claim_approved_amount
    FROM with_parent_adjuster s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(reported_at_year) OVER w_policy_id_reported_at_year AS policy_id_reported_at_year_min_reported_at_year,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id, reported_at_year)) AS distinct_status_in_policy_id_reported_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_team ORDER BY approved_amount DESC, id), 3) AS approved_amount_cume_dist,
        SUM(approved_amount) OVER (PARTITION BY adjuster_team ORDER BY approved_amount DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS approved_amount_sum_2
    FROM with_stats
    WINDOW w_policy_id_reported_at_year AS (PARTITION BY policy_id, reported_at_year)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        adjuster_name,
        adjuster_level,
        max_claim_approved_amount,
        policy_id_reported_at_year_min_reported_at_year,
        approved_amount_cume_dist,
        approved_amount_sum_2,
        CASE WHEN adjuster_team IN ('Health', 'Property') THEN 'group_a' WHEN adjuster_team = 'Travel' THEN 'group_b' ELSE 'other' END AS adjuster_team_group
    FROM with_windows
    WHERE approved_amount_sum_2 > 30000.0
),
unique_rows AS (
    -- Keep the latest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        adjuster_name,
        adjuster_level,
        max_claim_approved_amount,
        policy_id_reported_at_year_min_reported_at_year,
        approved_amount_cume_dist,
        approved_amount_sum_2,
        adjuster_team_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY approved_amount_sum_2 DESC, id DESC) AS recency_rank
        FROM derived
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per adjuster_team_group, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        adjuster_name,
        adjuster_level,
        max_claim_approved_amount,
        policy_id_reported_at_year_min_reported_at_year,
        approved_amount_cume_dist,
        approved_amount_sum_2,
        adjuster_team_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_team_group, adjuster_id ORDER BY approved_amount_sum_2 DESC, id DESC) AS recency_rank
        FROM unique_rows
    ) ranked
    WHERE recency_rank = 1
)
SELECT id, field, value
FROM (
    SELECT id, max_claim_approved_amount, approved_amount_cume_dist, approved_amount_sum_2
    FROM unique_rows_2
) src
UNPIVOT (
    value FOR field IN (max_claim_approved_amount, approved_amount_cume_dist, approved_amount_sum_2)
) unp

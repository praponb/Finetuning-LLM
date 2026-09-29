WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
    FROM claims
),
with_parent AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at_label,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM scoped_claims s
    JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(policy_premium) OVER (PARTITION BY policy_holder_id, adjuster_team ORDER BY approved_amount DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS policy_premium_avg_4,
        LAST_VALUE(policy_premium) OVER (PARTITION BY policy_holder_id ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_policy_premium,
        LEAD(approved_amount, 2) OVER w_policy_holder_id_adjuster_te_2 AS next2_approved_amount,
        SUM(approved_amount) OVER w_adjuster_team AS adjuster_team_sum_approved_amount,
        NTH_VALUE(policy_end_date, 2) OVER (PARTITION BY policy_holder_id, adjuster_team ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_end_date,
        ROUND(policy_premium / NULLIF(SUM(policy_premium) OVER (PARTITION BY adjuster_team), 0), 4) AS policy_premium_share_of_adjuster_team,
        FIRST_VALUE(policy_end_date) IGNORE NULLS OVER (PARTITION BY policy_holder_id, adjuster_team ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_end_date,
        MIN(policy_premium) OVER w_adjuster_team AS adjuster_team_min_policy_premium,
        COUNT(*) OVER (PARTITION BY policy_holder_id, adjuster_team ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(policy_end_date) IGNORE NULLS OVER (PARTITION BY policy_holder_id ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_policy_end_date_2,
        ROW_NUMBER() OVER w_policy_holder_id_adjuster_te_2 AS approved_amount_seq,
        LAST_VALUE(policy_premium) OVER (ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_policy_premium_2,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY policy_holder_id ORDER BY policy_premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        SIZE(COLLECT_SET(adjuster_team) OVER (PARTITION BY policy_id)) AS distinct_adjuster_team_in_policy_id
    FROM with_parent
    WINDOW w_policy_holder_id_adjuster_te_2 AS (PARTITION BY policy_holder_id, adjuster_team ORDER BY approved_amount DESC, id), w_adjuster_team AS (PARTITION BY adjuster_team)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at_label,
        policy_end_date,
        policy_premium,
        policy_holder_id,
        adjuster_name,
        adjuster_team,
        policy_premium_avg_4,
        last_policy_premium,
        next2_approved_amount,
        adjuster_team_sum_approved_amount,
        second_policy_end_date,
        policy_premium_share_of_adjuster_team,
        first_known_policy_end_date,
        adjuster_team_min_policy_premium,
        running_count,
        first_known_policy_end_date_2,
        last_policy_premium_2,
        second_approved_amount,
        distinct_adjuster_team_in_policy_id
    FROM with_windows
    WHERE approved_amount_seq <= 3
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at_label,
    policy_premium,
    policy_holder_id,
    adjuster_name,
    adjuster_team,
    policy_premium_avg_4,
    last_policy_premium,
    next2_approved_amount,
    adjuster_team_sum_approved_amount,
    second_policy_end_date,
    policy_premium_share_of_adjuster_team,
    first_known_policy_end_date,
    adjuster_team_min_policy_premium,
    running_count,
    distinct_adjuster_team_in_policy_id,
    DATEDIFF(DATE '2026-01-01', second_policy_end_date) AS days_since_second_policy_end_date
FROM leaders
WHERE approved_amount BETWEEN 0.0 AND 32510.7
ORDER BY policy_premium_avg_4 DESC

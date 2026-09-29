WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE status IS NOT NULL
),
with_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.claimed_amount_minus_approved_amount,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM prepared s
    JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_adjuster_id_policy_id_by_cla AS claimed_amount_minus_approved_amount_rank,
        MIN(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_team) AS claim_date_month_adjuster_team_min_approved_amou,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claimed_amount_minus_approved_amount DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        DENSE_RANK() OVER w_all_by_claimed_amount_minus AS claimed_amount_minus_approved_amount_dense_rank,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claimed_amount_minus_approved_amount DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_7,
        LAG(claim_date, 3) OVER (ORDER BY claimed_amount_minus_approved_amount ASC, id) AS prev3_claim_date,
        ROUND(AVG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_team), 2) AS claim_date_month_adjuster_team_avg_approved_amou,
        ROUND(approved_amount - AVG(approved_amount) OVER w_claim_date_month_adjuster_te, 2) AS approved_amount_vs_claim_date_month_adjuster_tea,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claimed_amount_minus_approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        LAG(approved_amount) OVER w_adjuster_id_policy_id_by_cla AS prev_approved_amount,
        LEAD(claim_date) OVER (ORDER BY claim_date, id) AS next_claim_date
    FROM with_details_adjuster
    WINDOW w_adjuster_id_policy_id_by_cla AS (PARTITION BY adjuster_id, policy_id ORDER BY claimed_amount_minus_approved_amount DESC, id), w_all_by_claimed_amount_minus AS (ORDER BY claimed_amount_minus_approved_amount ASC, id), w_claim_date_month_adjuster_te AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_team)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE claimed_amount_minus_approved_amount_rank <= 3
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_type,
        approved_amount,
        adjuster_name,
        adjuster_team,
        claim_date_month_adjuster_team_min_approved_amou,
        approved_amount_avg_4,
        claimed_amount_minus_approved_amount_dense_rank,
        prev3_claim_date,
        claim_date_month_adjuster_team_avg_approved_amou,
        approved_amount_vs_claim_date_month_adjuster_tea,
        LEAD(approved_amount) OVER w_all_by_prev_approved_amount AS next_approved_amount,
        FIRST_VALUE(prev3_claim_date) OVER (ORDER BY approved_amount_avg_4 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev3_claim_date,
        ROUND(prev_approved_amount - AVG(prev_approved_amount) OVER (PARTITION BY adjuster_id, adjuster_team), 2) AS prev_approved_amount_vs_adjuster_id_adjuster_tea,
        COUNT(*) OVER w_claim_date_month AS rows_in_claim_date_month
    FROM top_per_group
    WINDOW w_all_by_prev_approved_amount AS (ORDER BY prev_approved_amount DESC, id), w_claim_date_month AS (PARTITION BY MONTH(claim_date))
)
SELECT
    id,
    approved_amount,
    approved_amount_avg_4,
    claimed_amount_minus_approved_amount_dense_rank,
    prev3_claim_date,
    claim_date_month_adjuster_team_avg_approved_amou,
    next_approved_amount,
    prev_approved_amount_vs_adjuster_id_adjuster_tea,
    rows_in_claim_date_month
FROM windowed_2
ORDER BY prev3_claim_date, id
LIMIT 20

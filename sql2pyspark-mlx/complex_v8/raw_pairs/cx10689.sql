WITH base AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE claim_date >= DATE '2024-04-11'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(approved_amount) OVER (PARTITION BY status ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS approved_amount_max_3_prior,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_id_policy_id, 0), 4) AS approved_amount_share_of_adjuster_id_policy_id,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id_reported_at_year, 0), 4) AS claimed_amount_share_of_policy_id_reported_at_ye,
        ROUND(AVG(claimed_amount) OVER w_policy_id_reported_at_year, 2) AS policy_id_reported_at_year_avg_claimed_amount,
        MIN(claimed_amount) OVER (ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id_reported_at_year, 2) AS approved_amount_vs_policy_id_reported_at_year_av,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at ASC, id), 3) AS reported_at_percent_rank
    FROM base
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id), w_policy_id_reported_at_year AS (PARTITION BY policy_id, YEAR(reported_at))
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        claim_type,
        approved_amount,
        status,
        approved_amount_max_3_prior,
        approved_amount_share_of_adjuster_id_policy_id,
        claimed_amount_share_of_policy_id_reported_at_ye,
        min_claimed_amount_so_far,
        approved_amount_vs_policy_id_reported_at_year_av,
        reported_at_percent_rank,
        TO_DATE(reported_at) AS reported_at_date,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref
    FROM with_windows
    WHERE policy_id_reported_at_year_avg_claimed_amount <= 37525.0
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status)) AS distinct_claim_type_in_status,
        LAST_VALUE(approved_amount) OVER (ORDER BY claimed_amount_share_of_policy_id_reported_at_ye DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        ROUND(reported_at_percent_rank / NULLIF(SUM(reported_at_percent_rank) OVER (PARTITION BY status), 0), 4) AS reported_at_percent_rank_share_of_status,
        reported_at_percent_rank - LAG(reported_at_percent_rank) OVER (PARTITION BY claim_type, status ORDER BY min_claimed_amount_so_far ASC, id) AS reported_at_percent_rank_change
    FROM derived
)
-- Final projection
SELECT
    id,
    claim_type,
    approved_amount,
    status,
    approved_amount_max_3_prior,
    approved_amount_share_of_adjuster_id_policy_id,
    claimed_amount_share_of_policy_id_reported_at_ye,
    min_claimed_amount_so_far,
    approved_amount_vs_policy_id_reported_at_year_av,
    reported_at_date,
    claim_type_ref,
    distinct_claim_type_in_status,
    last_approved_amount,
    reported_at_percent_rank_share_of_status,
    reported_at_percent_rank_change
FROM with_windows_2
ORDER BY claimed_amount_share_of_policy_id_reported_at_ye, reported_at_percent_rank_share_of_status

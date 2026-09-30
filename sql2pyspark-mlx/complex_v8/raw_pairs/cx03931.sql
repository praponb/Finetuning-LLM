WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        CASE WHEN claimed_amount >= 41878.5 THEN 'large' WHEN claimed_amount >= 19337.6 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE status IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(reported_at) ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        RANK() OVER w_reported_at_month_by_reporte_2 AS reported_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY approved_amount ASC), 3) AS approved_amount_cume_dist
    FROM scoped
    WINDOW w_reported_at_month_by_reporte_2 AS (PARTITION BY MONTH(reported_at) ORDER BY reported_at)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        claim_type_or_unknown,
        claimed_amount_tier,
        reported_at_rank,
        approved_amount_cume_dist,
        MAX(reported_at_percent_rank) OVER (PARTITION BY adjuster_id, claim_type_or_unknown ORDER BY reported_at_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS reported_at_percent_rank_max_3_prior,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_id, reported_at_rank ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, reported_at_rank ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        ROUND(AVG(approved_amount_cume_dist) OVER w_reported_at_month_claim_type, 2) AS reported_at_month_claim_type_or_unknown_avg_appr,
        LAG(reported_at_rank) OVER w_policy_id_reported_at_rank_b_3 AS prev_reported_at_rank
    FROM with_windows
    WINDOW w_reported_at_month_claim_type AS (PARTITION BY MONTH(reported_at), claim_type_or_unknown), w_policy_id_reported_at_rank_b_3 AS (PARTITION BY policy_id, reported_at_rank ORDER BY approved_amount, id)
),
adjuster_id_stats AS (
    -- Avg reported_at_rank per adjuster_id
    SELECT adjuster_id, AVG(reported_at_rank) AS avg_reported_at_rank
    FROM windowed
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_reported_at_rank,
        ROUND(s.reported_at_rank - g.avg_reported_at_rank, 2) AS reported_at_rank_minus_avg
    FROM windowed s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.reported_at_rank > g.avg_reported_at_rank
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount_tier,
        reported_at_rank,
        approved_amount_cume_dist,
        reported_at_percent_rank_max_3_prior,
        reported_at_month_claim_type_or_unknown_avg_appr,
        avg_reported_at_rank,
        reported_at_rank_minus_avg,
        LOWER(REGEXP_REPLACE(TRIM(claimed_amount_tier), ' ', '_')) AS claimed_amount_tier_slug
    FROM outliers
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount_tier,
    reported_at_rank,
    approved_amount_cume_dist,
    avg_reported_at_rank,
    reported_at_rank_minus_avg,
    claimed_amount_tier_slug,
    INITCAP(TRIM(claimed_amount_tier_slug)) AS claimed_amount_tier_slug_clean
FROM labeled
ORDER BY reported_at_rank, policy_id DESC

WITH recent_claims AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE claim_date >= DATE '2024-03-24'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER w_adjuster_id) AS distinct_claim_type_in_adjuster_id,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_7,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY claim_type ORDER BY reported_at, id)) AS days_since_prev_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id, 2) AS approved_amount_vs_adjuster_id_avg,
        RANK() OVER (PARTITION BY MONTH(claim_date), adjuster_id ORDER BY reported_at) AS reported_at_rank,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY status ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        COUNT(*) OVER (PARTITION BY MONTH(claim_date), adjuster_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id) AS reported_at_decile,
        LAST_VALUE(reported_at) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY status ORDER BY claim_date, id) AS claimed_amount_change,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY reported_at, id), 3) AS reported_at_cume_dist
    FROM recent_claims
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
),
top_per_group AS (
    -- Top 5 per group
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
        distinct_claim_type_in_adjuster_id,
        approved_amount_avg_7,
        days_since_prev_claim_date,
        approved_amount_vs_adjuster_id_avg,
        distinct_policy_id_in_adjuster_id,
        rows_in_adjuster_id,
        first_known_approved_amount,
        running_count,
        reported_at_decile,
        last_reported_at,
        reported_at_percent_rank,
        claimed_amount_change,
        reported_at_cume_dist
    FROM with_trends
    WHERE reported_at_rank <= 5
      AND distinct_policy_id_in_adjuster_id <= 8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        distinct_claim_type_in_adjuster_id,
        approved_amount_avg_7,
        days_since_prev_claim_date,
        distinct_policy_id_in_adjuster_id,
        rows_in_adjuster_id,
        first_known_approved_amount,
        running_count,
        reported_at_decile,
        last_reported_at,
        reported_at_percent_rank,
        claimed_amount_change,
        reported_at_cume_dist,
        LAG(reported_at_percent_rank, 3, 0) OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at_cume_dist NULLS LAST, id) AS prev3_reported_at_percent_rank,
        ROUND(reported_at_percent_rank / NULLIF(SUM(reported_at_percent_rank) OVER (PARTITION BY MONTH(claim_date)), 0), 4) AS reported_at_percent_rank_share_of_claim_date_mon,
        NTH_VALUE(first_known_approved_amount, 2) OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at_cume_dist NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_known_approved_amount
    FROM top_per_group
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(reported_at) AS reported_at_year,
        claim_type,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        SUM(days_since_prev_claim_date) AS total_days_since_prev_claim_date
    FROM ranked
    GROUP BY YEAR(reported_at), claim_type
)
SELECT
    reported_at_year,
    claim_type,
    pct_status_is_not_null,
    total_days_since_prev_claim_date
FROM totals
WHERE total_days_since_prev_claim_date < 840
ORDER BY total_days_since_prev_claim_date DESC

WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label
    FROM claims
    WHERE policy_id <= 16
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(approved_amount) OVER (PARTITION BY status ORDER BY reported_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id)) AS distinct_policy_id_in_adjuster_id,
        LAG(claim_date) OVER (PARTITION BY claim_type ORDER BY reported_at, id) AS prev_claim_date,
        MIN(approved_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_min_approved_amount,
        LEAD(reported_at) OVER (PARTITION BY status ORDER BY reported_at NULLS LAST, id) AS next_reported_at,
        FIRST_VALUE(claim_date_quarter) OVER (PARTITION BY status ORDER BY reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_quarter,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at) AS reported_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY YEAR(reported_at), adjuster_id ORDER BY reported_at) AS reported_at_dense_rank_2,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_approved_amount,
        MAX(approved_amount) OVER (PARTITION BY claim_type ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        FIRST_VALUE(claim_date_quarter) OVER (PARTITION BY claim_type ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_quarter_2,
        LAST_VALUE(claim_date_quarter) OVER (PARTITION BY claim_type ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date_quarter,
        LEAD(claim_date, 3) OVER (PARTITION BY YEAR(reported_at), adjuster_id ORDER BY reported_at, id) AS next3_claim_date
    FROM valid
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        approved_amount_sum_6_prior,
        rows_in_adjuster_id,
        distinct_policy_id_in_adjuster_id,
        prev_claim_date,
        adjuster_id_min_approved_amount,
        next_reported_at,
        first_claim_date_quarter,
        reported_at_dense_rank,
        reported_at_dense_rank_2,
        adjuster_id_sum_approved_amount,
        first_claim_date_quarter_2,
        last_claim_date_quarter,
        next3_claim_date,
        GREATEST(rows_in_adjuster_id, reported_at_dense_rank) AS greatest_rows_in_adjuster_id_reported_at_dense_r
    FROM with_trends
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(last_claim_date_quarter, 'yyyy-MM') AS last_claim_date_quarter_ym,
        DATE_FORMAT(next_reported_at, 'yyyy-MM') AS next_reported_at_ym,
        status,
        SUM(adjuster_id_sum_approved_amount) AS total_adjuster_id_sum_approved_amount,
        COUNT(*) FILTER (WHERE claim_type = 'flood') AS n_claim_type_flood,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        SUM(reported_at_dense_rank_2) FILTER (WHERE claim_type = 'flood') AS reported_at_dense_rank_2_claim_type_flood,
        SUM(CASE WHEN status IN ('APPROVED', 'IN_REVIEW', 'PAID') THEN 1 ELSE 0 END) AS n_status_in_approved_in_review
    FROM enriched
    WHERE next3_claim_date < DATE '2025-05-30'
    GROUP BY DATE_FORMAT(last_claim_date_quarter, 'yyyy-MM'), DATE_FORMAT(next_reported_at, 'yyyy-MM'), status
    HAVING SUM(reported_at_dense_rank) > 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        last_claim_date_quarter_ym,
        next_reported_at_ym,
        status,
        total_adjuster_id_sum_approved_amount,
        n_claim_type_flood,
        claim_type_list,
        ROUND(AVG(distinct_adjuster_ids) OVER (ORDER BY total_adjuster_id_sum_approved_amount, last_claim_date_quarter_ym, next_reported_at_ym, status ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_adjuster_ids_avg_1_prior,
        ROUND(distinct_adjuster_ids / NULLIF(SUM(distinct_adjuster_ids) OVER (PARTITION BY status, n_claim_type_flood), 0), 4) AS distinct_adjuster_ids_share_of_status_n_claim_ty
    FROM grouped
)
SELECT
    last_claim_date_quarter_ym,
    next_reported_at_ym,
    status,
    total_adjuster_id_sum_approved_amount,
    n_claim_type_flood,
    distinct_adjuster_ids_avg_1_prior,
    distinct_adjuster_ids_share_of_status_n_claim_ty,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM windowed
ORDER BY next_reported_at_ym DESC

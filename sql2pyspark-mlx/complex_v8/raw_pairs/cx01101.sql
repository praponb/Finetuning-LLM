WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at,
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE approved_amount <= 14377.6
      AND claim_date IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_policy_id_by_approved_amount AS approved_amount_rank,
        NTILE(4) OVER (PARTITION BY policy_id ORDER BY approved_amount DESC NULLS FIRST, id) AS approved_amount_quartile,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date_month_start)), 2) AS approved_amount_vs_policy_id_claim_date_month_st,
        LAST_VALUE(approved_amount) OVER (PARTITION BY YEAR(claim_date_month_start) ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, MONTH(claim_date_month_start))) AS distinct_claim_type_in_policy_id_claim_date_mont,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, MONTH(claim_date_month_start)), 2) AS policy_id_claim_date_month_start_month_avg_appro
    FROM recent
    WINDOW w_policy_id_by_approved_amount AS (PARTITION BY policy_id ORDER BY approved_amount DESC NULLS FIRST, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(last_approved_amount, approved_amount) AS greatest_last_approved_amount_approved_amount,
        ABS(approved_amount_rank - 4) AS approved_amount_rank_distance
    FROM with_windows
)
SELECT
    approved_amount_rank_distance,
    approved_amount_quartile,
    policy_id,
    COUNT(*) AS cnt,
    ROUND(AVG(last_approved_amount), 2) AS avg_last_approved_amount,
    SUM(CASE WHEN claim_type IN ('flood', 'theft', 'delay') THEN 1 ELSE 0 END) AS n_claim_type_in_flood_theft_de
FROM annotated
GROUP BY ROLLUP(approved_amount_rank_distance, approved_amount_quartile, policy_id)

WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE policy_id <= 12
),
combined_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.reported_at_date,
        s.claim_type_len,
        p.end_date AS policy_end_date,
        po.risk_score AS holder_risk_score,
        po.region AS holder_region
    FROM filtered s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(holder_region) OVER (PARTITION BY policy_id, claim_type_len)) AS distinct_holder_region_in_policy_id_claim_type_l,
        LAST_VALUE(claim_date) OVER (ORDER BY claim_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        COUNT(*) OVER (PARTITION BY policy_id, claim_type_len) AS rows_in_policy_id_claim_type_len,
        MIN(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY claim_date ASC, id)) AS days_since_prev_claim_date,
        FIRST_VALUE(holder_risk_score) IGNORE NULLS OVER (PARTITION BY claim_type_len, policy_id ORDER BY policy_end_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_risk_score,
        RANK() OVER (ORDER BY claim_date ASC) AS claim_date_rank,
        MAX(holder_risk_score) OVER (PARTITION BY adjuster_id, claim_type_len) AS adjuster_id_claim_type_len_max_holder_risk_score,
        ROUND(claim_type_len / NULLIF(SUM(claim_type_len) OVER (PARTITION BY policy_id, claim_type_len), 0), 4) AS claim_type_len_share_of_policy_id_claim_type_len
    FROM combined_details
),
first_rows AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE claim_date_rank <= 3
      AND holder_risk_score < 0.6
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(claim_date_rank) OVER (PARTITION BY distinct_holder_region_in_policy_id_claim_type_l, YEAR(reported_at_date) ORDER BY distinct_holder_region_in_policy_id_claim_type_l DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claim_date_rank_avg_4,
        ROUND(days_since_prev_claim_date / NULLIF(SUM(days_since_prev_claim_date) OVER w_reported_at_date_month, 0), 4) AS days_since_prev_claim_date_share_of_reported_at_,
        ROUND(AVG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at_date)), 2) AS reported_at_date_month_avg_approved_amount,
        DATEDIFF(reported_at_date, LAG(reported_at_date) OVER (PARTITION BY distinct_holder_region_in_policy_id_claim_type_l, YEAR(reported_at_date) ORDER BY distinct_holder_region_in_policy_id_claim_type_l DESC, id)) AS days_since_prev_reported_at_date
    FROM first_rows
    WINDOW w_reported_at_date_month AS (PARTITION BY DATE_TRUNC('MONTH', reported_at_date))
),
per_group AS (
    -- Group-level totals
    SELECT
        claim_type_len,
        claim_date_rank,
        DATE_FORMAT(last_claim_date, 'yyyy-MM') AS last_claim_date_ym,
        ARRAY_SORT(COLLECT_SET(holder_region)) AS holder_region_list,
        MAX(claim_date) AS last_claim_date_2,
        ROUND(AVG(adjuster_id_claim_type_len_max_holder_risk_score), 2) AS avg_adjuster_id_claim_type_len_max_holder_risk_s,
        SUM(min_approved_amount_so_far) AS total_min_approved_amount_so_far,
        COUNT(*) AS n_rows
    FROM windowed
    GROUP BY claim_type_len, claim_date_rank, DATE_FORMAT(last_claim_date, 'yyyy-MM')
    HAVING AVG(claim_date_rank) > 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        claim_type_len,
        claim_date_rank,
        last_claim_date_ym,
        holder_region_list,
        avg_adjuster_id_claim_type_len_max_holder_risk_s,
        total_min_approved_amount_so_far,
        n_rows,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', last_claim_date_2), claim_type_len ORDER BY n_rows DESC NULLS FIRST), 3) AS n_rows_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY last_claim_date_2 NULLS LAST, claim_type_len, claim_date_rank, last_claim_date_ym), 3) AS last_claim_date_2_cume_dist,
        LEAD(claim_type_len) OVER w_last_claim_date_2_month_clai_2 AS next_claim_type_len,
        RANK() OVER (ORDER BY last_claim_date_2 NULLS LAST) AS last_claim_date_2_rank
    FROM per_group
    WINDOW w_last_claim_date_2_month_clai_2 AS (PARTITION BY DATE_TRUNC('MONTH', last_claim_date_2), claim_type_len ORDER BY n_rows DESC NULLS FIRST, claim_type_len, claim_date_rank, last_claim_date_ym)
)
-- Final projection
SELECT
    claim_type_len,
    claim_date_rank,
    last_claim_date_ym,
    avg_adjuster_id_claim_type_len_max_holder_risk_s,
    total_min_approved_amount_so_far,
    n_rows,
    n_rows_cume_dist,
    last_claim_date_2_cume_dist,
    next_claim_type_len,
    last_claim_date_2_rank
FROM with_windows
ORDER BY last_claim_date_2_cume_dist, avg_adjuster_id_claim_type_len_max_holder_risk_s DESC

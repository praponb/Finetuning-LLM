WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE claim_date < DATE '2024-10-22'
      AND policy_id <= 28
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        approved_amount_minus_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY reported_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        DENSE_RANK() OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY reported_at ASC, id) AS reported_at_dense_rank,
        COUNT(*) OVER (PARTITION BY claim_type, policy_id) AS rows_in_claim_type_policy_id,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY reported_at ASC, id), 3) AS reported_at_cume_dist,
        COUNT(*) OVER (PARTITION BY claim_type, status ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY reported_at ASC, id)) AS days_since_prev_claim_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY reported_at, id) AS reported_at_seq,
        SUM(claimed_amount) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_3_prior,
        LEAD(claimed_amount, 3, 0) OVER (PARTITION BY status ORDER BY reported_at, id) AS next3_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY claim_type, policy_id), 2) AS claimed_amount_vs_claim_type_policy_id_avg,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY claim_type, status ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type, status ORDER BY reported_at ASC, id), 3) AS reported_at_cume_dist_2
    FROM base
    WINDOW w_claim_type_policy_id AS (PARTITION BY claim_type, policy_id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        approved_amount_minus_claimed_amount,
        claimed_amount_max_1_prior,
        reported_at_dense_rank,
        rows_in_claim_type_policy_id,
        reported_at_cume_dist,
        running_count,
        days_since_prev_claim_date,
        claimed_amount_sum_3_prior,
        next3_claimed_amount,
        claimed_amount_vs_claim_type_policy_id_avg,
        second_reported_at,
        reported_at_cume_dist_2
    FROM metrics
    WHERE reported_at_seq <= 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        approved_amount_minus_claimed_amount,
        claimed_amount_max_1_prior,
        reported_at_dense_rank,
        rows_in_claim_type_policy_id,
        reported_at_cume_dist,
        days_since_prev_claim_date,
        claimed_amount_sum_3_prior,
        next3_claimed_amount,
        claimed_amount_vs_claim_type_policy_id_avg,
        second_reported_at,
        reported_at_cume_dist_2,
        SUM(claimed_amount) OVER (PARTITION BY status, policy_id ORDER BY reported_at_cume_dist NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_7,
        COUNT(*) OVER (PARTITION BY TO_DATE(second_reported_at), reported_at_dense_rank) AS rows_in_second_reported_at_to_date_reported_at_d,
        NTILE(5) OVER (PARTITION BY status, policy_id ORDER BY reported_at_cume_dist NULLS LAST, id) AS reported_at_cume_dist_quintile,
        ROUND(reported_at_cume_dist - AVG(reported_at_cume_dist) OVER w_second_reported_at_to_date_r, 2) AS reported_at_cume_dist_vs_second_reported_at_to_d,
        MIN(reported_at_dense_rank) OVER (PARTITION BY TO_DATE(second_reported_at), reported_at_dense_rank) AS second_reported_at_to_date_reported_at_dense_ran
    FROM top_rows
    WINDOW w_second_reported_at_to_date_r AS (PARTITION BY TO_DATE(second_reported_at), reported_at_dense_rank)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        MIN(second_reported_at_to_date_reported_at_dense_ran) AS min_second_reported_at_to_date_reported_at_dense,
        COUNT(*) AS cnt,
        ROUND(SUM(next3_claimed_amount) / NULLIF(SUM(second_reported_at_to_date_reported_at_dense_ran), 0), 3) AS next3_claimed_amount_to_second_reported_at_to_da,
        ROUND(SUM(second_reported_at_to_date_reported_at_dense_ran) / NULLIF(SUM(rows_in_claim_type_policy_id), 0), 3) AS second_reported_at_to_date_reported_at_dense_ran_2
    FROM with_windows
    WHERE approved_amount_minus_claimed_amount >= 25000.0
    GROUP BY 1
    HAVING MAX(claimed_amount_sum_3_prior) > 37575.0
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY distinct_policy_ids, policy_id ORDER BY second_reported_at_to_date_reported_at_dense_ran_2 ASC NULLS LAST) AS second_reported_at_to_date_reported_at_dense_ran,
        cnt - LAG(cnt) OVER (PARTITION BY distinct_policy_ids, policy_id ORDER BY second_reported_at_to_date_reported_at_dense_ran_2 ASC NULLS LAST, policy_id) AS cnt_change,
        ROUND(AVG(cnt) OVER (PARTITION BY distinct_policy_ids, policy_id ORDER BY second_reported_at_to_date_reported_at_dense_ran_2 ASC NULLS LAST, policy_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS cnt_avg_1_prior
    FROM per_group
)
SELECT
    policy_id,
    distinct_policy_ids,
    cnt,
    next3_claimed_amount_to_second_reported_at_to_da,
    second_reported_at_to_date_reported_at_dense_ran_2,
    second_reported_at_to_date_reported_at_dense_ran,
    cnt_avg_1_prior,
    GREATEST(cnt_avg_1_prior, distinct_policy_ids) AS greatest_cnt_avg_1_prior_distinct_policy_ids
FROM sequenced
WHERE second_reported_at_to_date_reported_at_dense_ran <= 6
ORDER BY next3_claimed_amount_to_second_reported_at_to_da NULLS LAST

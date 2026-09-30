WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-02-12'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY claimed_amount DESC NULLS FIRST) AS claimed_amount_dense_rank,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC NULLS FIRST) AS claimed_amount_dense_rank_2,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        MIN(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_min_claimed_amount,
        NTILE(5) OVER (ORDER BY claimed_amount DESC NULLS FIRST, id) AS claimed_amount_quintile,
        MAX(claimed_amount) OVER w_reported_at_year AS reported_at_year_max_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER w_reported_at_year) AS distinct_claim_type_in_reported_at_year,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY YEAR(reported_at)), 0), 4) AS claimed_amount_share_of_reported_at_year,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_claimed_amount,
        SUM(claimed_amount) OVER (ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_3_prior,
        NTH_VALUE(reported_at, 2) OVER (ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_2,
        RANK() OVER (ORDER BY reported_at DESC) AS reported_at_rank,
        MIN(claimed_amount) OVER (PARTITION BY YEAR(reported_at)) AS reported_at_year_min_claimed_amount,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount
    FROM cleaned_claims
    WINDOW w_reported_at_year AS (PARTITION BY YEAR(reported_at))
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        claimed_amount_dense_rank,
        second_reported_at,
        adjuster_id_min_claimed_amount,
        claimed_amount_quintile,
        reported_at_year_max_claimed_amount,
        distinct_claim_type_in_reported_at_year,
        claimed_amount_share_of_reported_at_year,
        adjuster_id_sum_claimed_amount,
        claimed_amount_sum_3_prior,
        second_reported_at_2,
        reported_at_rank,
        reported_at_year_min_claimed_amount,
        second_claimed_amount
    FROM sequenced
    WHERE claimed_amount_dense_rank_2 = 1
      AND reported_at_year_max_claimed_amount >= 50090.0
),
totals AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        reported_at_rank,
        claim_type,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MIN(second_claimed_amount) AS min_second_claimed_amount,
        ROUND(100 * COUNT(CASE WHEN claim_type NOT IN ('collision', 'fire') THEN 1 END) / COUNT(*), 1) AS pct_claim_type_not_in_collision,
        ROUND(SUM(adjuster_id_min_claimed_amount), 2) AS total_adjuster_id_min_claimed_amount,
        ROUND(AVG(claimed_amount_share_of_reported_at_year), 2) AS avg_claimed_amount_share_of_reported_at_year
    FROM top_rows
    GROUP BY adjuster_id, reported_at_rank, claim_type
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(avg_claimed_amount - AVG(avg_claimed_amount) OVER w_distinct_adjuster_ids, 2) AS avg_claimed_amount_vs_distinct_adjuster_ids_avg,
        LAG(reported_at_rank, 1, 0) OVER (PARTITION BY distinct_adjuster_ids ORDER BY avg_claimed_amount, adjuster_id, reported_at_rank, claim_type) AS prev_reported_at_rank
    FROM totals
    WINDOW w_distinct_adjuster_ids AS (PARTITION BY distinct_adjuster_ids)
)
-- Final projection
SELECT
    adjuster_id,
    avg_claimed_amount,
    distinct_adjuster_ids,
    min_second_claimed_amount,
    pct_claim_type_not_in_collision,
    total_adjuster_id_min_claimed_amount,
    avg_claimed_amount_share_of_reported_at_year,
    avg_claimed_amount_vs_distinct_adjuster_ids_avg
FROM with_trends
WHERE avg_claimed_amount_share_of_reported_at_year > 0.2
ORDER BY avg_claimed_amount

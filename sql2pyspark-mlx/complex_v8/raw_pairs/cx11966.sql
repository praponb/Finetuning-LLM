WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        INITCAP(TRIM(status)) AS status_clean,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount,
        CASE WHEN claim_type IN ('flood', 'collision') THEN 'group_a' WHEN claim_type = 'theft' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
),
summary AS (
    -- Summarise per group
    SELECT
        claim_type_group,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount_per_approved_amount), 0), 3) AS approved_amount_to_claimed_amount_per_approved_a,
        ROUND(100 * COUNT(CASE WHEN claim_type = 'delay' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_delay
    FROM recent_claims
    GROUP BY claim_type_group
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_type_group) AS rows_in_claim_type_group,
        NTH_VALUE(pct_claim_type_delay, 2) OVER (PARTITION BY claim_type_group ORDER BY pct_claim_type_delay, claim_type_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_pct_claim_type_delay,
        NTILE(4) OVER (ORDER BY pct_claim_type_delay DESC, claim_type_group) AS pct_claim_type_delay_quartile
    FROM summary
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE second_pct_claim_type_delay < (SELECT MAX(second_pct_claim_type_delay) FROM ranked)
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        claim_type_group,
        approved_amount_to_claimed_amount_per_approved_a,
        pct_claim_type_delay,
        rows_in_claim_type_group,
        pct_claim_type_delay_quartile,
        ROW_NUMBER() OVER (PARTITION BY pct_claim_type_delay_quartile ORDER BY approved_amount_to_claimed_amount_per_approved_a DESC NULLS FIRST, claim_type_group) AS approved_amount_to_claimed_amount_per_approved_a_2,
        ROUND(rows_in_claim_type_group - AVG(rows_in_claim_type_group) OVER (PARTITION BY pct_claim_type_delay_quartile, claim_type_group), 2) AS rows_in_claim_type_group_vs_pct_claim_type_delay,
        ROUND(AVG(second_pct_claim_type_delay) OVER (PARTITION BY pct_claim_type_delay_quartile, claim_type_group ORDER BY pct_claim_type_delay_quartile DESC, claim_type_group ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS second_pct_claim_type_delay_avg_2,
        DENSE_RANK() OVER (PARTITION BY pct_claim_type_delay_quartile, claim_type_group ORDER BY pct_claim_type_delay_quartile DESC) AS pct_claim_type_delay_quartile_dense_rank,
        ROUND(pct_claim_type_delay / NULLIF(SUM(pct_claim_type_delay) OVER w_pct_claim_type_delay_quartil_2, 0), 4) AS pct_claim_type_delay_share_of_pct_claim_type_del
    FROM without_matches
    WINDOW w_pct_claim_type_delay_quartil_2 AS (PARTITION BY pct_claim_type_delay_quartile, claim_type_group)
)
SELECT
    claim_type_group,
    pct_claim_type_delay_quartile,
    pct_claim_type_delay_quartile_dense_rank,
    ROUND(STDDEV(rows_in_claim_type_group), 2) AS stddev_rows_in_claim_type_group,
    COUNT(DISTINCT claim_type_group) AS distinct_claim_type_groups
FROM ranked_2
GROUP BY ROLLUP(claim_type_group, pct_claim_type_delay_quartile, pct_claim_type_delay_quartile_dense_rank)
ORDER BY claim_type_group NULLS LAST, pct_claim_type_delay_quartile NULLS LAST, pct_claim_type_delay_quartile_dense_rank NULLS LAST

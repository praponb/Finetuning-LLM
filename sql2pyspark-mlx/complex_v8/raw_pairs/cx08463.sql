WITH scoped_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claimed_amount >= 5313.8
),
rollup_base AS (
    -- Group-level totals
    SELECT
        claim_type,
        YEAR(reported_at) AS reported_at_year,
        SUM(approved_amount) AS total_approved_amount,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM scoped_claims
    GROUP BY claim_type, YEAR(reported_at)
    HAVING COUNT(*) >= 3
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE n_rows < (SELECT MAX(n_rows) FROM rollup_base)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(n_rows, 2) OVER (PARTITION BY distinct_adjuster_ids, claim_type ORDER BY total_approved_amount DESC, claim_type, reported_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows,
        FIRST_VALUE(total_approved_amount) OVER (PARTITION BY distinct_adjuster_ids, claim_type ORDER BY total_approved_amount DESC, claim_type, reported_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_approved_amount,
        SUM(distinct_adjuster_ids) OVER (PARTITION BY reported_at_year, n_rows ORDER BY n_rows DESC, claim_type, reported_at_year ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_adjuster_ids_sum_6_prior,
        ROUND(median_claimed_amount - AVG(median_claimed_amount) OVER (PARTITION BY distinct_adjuster_ids), 2) AS median_claimed_amount_vs_distinct_adjuster_ids_a
    FROM qualified
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reported_at_year, claim_type, median_claimed_amount_vs_distinct_adjuster_ids_a
    FROM with_windows
) src
PIVOT (
    SUM(median_claimed_amount_vs_distinct_adjuster_ids_a) AS amount, COUNT(median_claimed_amount_vs_distinct_adjuster_ids_a) AS n
    FOR claim_type IN ('theft', 'collision', 'flood')
)

WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        UPPER(TRIM(claim_type)) AS claim_type_clean_2,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM claims
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(reported_at) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date ASC, id) AS prev_reported_at,
        SUM(claimed_amount) OVER w_claim_date_month_adjuster_id AS claim_date_month_adjuster_id_sum_claimed_amount,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(claim_date, 2) OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date ASC, id) AS prev2_claim_date,
        RANK() OVER (PARTITION BY adjuster_id, policy_id ORDER BY claim_date ASC, id) AS claim_date_rank,
        RANK() OVER w_adjuster_id_policy_id_by_cla_3 AS claim_date_rank_2
    FROM valid
    WINDOW w_claim_date_month_adjuster_id AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id), w_adjuster_id_policy_id_by_cla_3 AS (PARTITION BY adjuster_id, policy_id ORDER BY claim_date ASC)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', prev_reported_at) AS prev_reported_at_month,
        YEAR(prev2_claim_date) AS prev2_claim_date_year,
        YEAR(claim_date) AS claim_date_year,
        ROUND(SUM(claim_date_month_adjuster_id_sum_claimed_amount) / NULLIF(SUM(claim_date_rank_2), 0), 3) AS claim_date_month_adjuster_id_sum_claimed_amount_,
        MIN(prev2_claim_date) AS first_prev2_claim_date,
        PERCENTILE_APPROX(claim_date_month_adjuster_id_sum_claimed_amount, 0.5) AS median_claim_date_month_adjuster_id_sum_claimed_,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(running_count) AS total_running_count,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount
    FROM ranked
    GROUP BY DATE_TRUNC('MONTH', prev_reported_at), YEAR(prev2_claim_date), YEAR(claim_date)
    HAVING SUM(claim_date_rank_2) > 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        prev_reported_at_month,
        prev2_claim_date_year,
        claim_date_year,
        claim_date_month_adjuster_id_sum_claimed_amount_,
        total_running_count,
        avg_claimed_amount,
        MAX(distinct_ids) OVER (ORDER BY total_running_count ASC, prev_reported_at_month, prev2_claim_date_year, claim_date_year ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_ids_max_6_prior,
        COUNT(*) OVER (PARTITION BY distinct_ids ORDER BY first_prev2_claim_date, prev_reported_at_month, prev2_claim_date_year, claim_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM stats
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE claim_date_month_adjuster_id_sum_claimed_amount_ > (SELECT MIN(claim_date_month_adjuster_id_sum_claimed_amount_) FROM sequenced)
)
-- Native SQL UNPIVOT (wide to long)
SELECT prev_reported_at_month, prev2_claim_date_year, field, amount
FROM (
    SELECT prev_reported_at_month, prev2_claim_date_year, avg_claimed_amount, claim_date_month_adjuster_id_sum_claimed_amount_
    FROM filtered
) src
UNPIVOT (
    amount FOR field IN (avg_claimed_amount AS avg_claimed_amount_v, claim_date_month_adjuster_id_sum_claimed_amount_ AS m_claim_date_month_adjuster_id_sum_claimed_amoun)
) unp

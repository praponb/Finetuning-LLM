WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label
    FROM claims
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_reported_at AS reported_at_dense_rank,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        claimed_amount - LAG(claimed_amount) OVER w_claim_type_by_reported_at AS claimed_amount_change,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY reported_at ASC, id) AS seq_no,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type) AS distinct_policy_id_in_claim_type,
        MAX(approved_amount) OVER (PARTITION BY policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far,
        NTILE(5) OVER (PARTITION BY claim_type ORDER BY reported_at ASC, id) AS reported_at_quintile,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        RANK() OVER w_policy_id_by_claim_date AS claim_date_rank,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claim_date ASC, id) AS claim_date_seq
    FROM valid
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at DESC, id), w_claim_type_by_reported_at AS (PARTITION BY claim_type ORDER BY reported_at ASC, id), w_claim_type AS (PARTITION BY claim_type), w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date ASC, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(claimed_amount_change, 3) OVER w_policy_id_claim_date_rank_by AS prev3_claimed_amount_change,
        FIRST_VALUE(first_known_claimed_amount) OVER (ORDER BY reported_at_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_known_claimed_amount,
        SIZE(COLLECT_SET(policy_id) OVER w_reported_at_year) AS distinct_policy_id_in_reported_at_year,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY MONTH(claim_date))) AS distinct_adjuster_id_in_claim_date_month
    FROM windowed
    WINDOW w_policy_id_claim_date_rank_by AS (PARTITION BY policy_id, claim_date_rank ORDER BY claimed_amount ASC, id), w_reported_at_year AS (PARTITION BY YEAR(reported_at))
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        reported_at_quintile,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        distinct_policy_id_in_claim_type,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        ROUND(AVG(max_approved_amount_so_far), 2) AS avg_max_approved_amount_so_far
    FROM with_trends
    WHERE approved_amount >= 9389.3
    GROUP BY reported_at_quintile, DATE_TRUNC('MONTH', claim_date), distinct_policy_id_in_claim_type
    HAVING MAX(distinct_policy_id_in_reported_at_year) > 3
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY distinct_adjuster_ids ASC) AS distinct_adjuster_ids_rank,
        NTH_VALUE(reported_at_quintile, 2) OVER (ORDER BY distinct_adjuster_ids ASC, reported_at_quintile, claim_date_month, distinct_policy_id_in_claim_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_quintile,
        MIN(n_rows) OVER (PARTITION BY distinct_adjuster_ids, distinct_policy_id_in_claim_type) AS distinct_adjuster_ids_distinct_policy_id_in_clai
    FROM summary
)
SELECT
    reported_at_quintile,
    claim_date_month,
    distinct_policy_id_in_claim_type,
    n_rows,
    distinct_adjuster_ids,
    avg_max_approved_amount_so_far,
    distinct_adjuster_ids_rank,
    HOUR(claim_date_month) AS claim_date_month_hour
FROM with_trends_2
WHERE claim_date_month < TIMESTAMP '2025-02-13 09:00:00'

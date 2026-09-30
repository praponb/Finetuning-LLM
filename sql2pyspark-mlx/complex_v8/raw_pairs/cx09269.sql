WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 42838.9 THEN 'HIGH' WHEN claimed_amount >= 13635.1 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM claims
    WHERE claimed_amount > 12726.7
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_claimed_amount
    FROM cleaned s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY claimed_amount_tier ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id, MONTH(claim_date_year) ORDER BY total_claim_claimed_amount), 3) AS total_claim_claimed_amount_percent_rank,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_claimed_amount_tier_by_repor_2)) / 60, 1) AS minutes_since_prev_reported_at,
        MAX(total_claim_claimed_amount) OVER (PARTITION BY claimed_amount_tier, claim_type) AS claimed_amount_tier_claim_type_max_total_claim_c,
        SUM(total_claim_claimed_amount) OVER w_claimed_amount_tier_claim_ty AS claimed_amount_tier_claim_type_sum_total_claim_c
    FROM summarized_rows
    WINDOW w_claimed_amount_tier_by_repor_2 AS (PARTITION BY claimed_amount_tier ORDER BY reported_at ASC, id), w_claimed_amount_tier_claim_ty AS (PARTITION BY claimed_amount_tier, claim_type)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        SUM(claimed_amount) FILTER (WHERE status IS NOT NULL) AS claimed_amount_status_is_not_null,
        COUNT(*) FILTER (WHERE status = 'PAID') AS n_status_paid,
        PERCENTILE_APPROX(total_claim_claimed_amount_percent_rank, 0.5) AS median_total_claim_claimed_amount_percent_rank,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(*) AS cnt
    FROM windowed
    GROUP BY DATE_FORMAT(claim_date, 'yyyy-MM')
    HAVING MAX(total_claim_claimed_amount) > 60080.0
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE median_total_claim_claimed_amount_percent_rank > (SELECT AVG(median_total_claim_claimed_amount_percent_rank) FROM rollup_base)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(cnt / NULLIF(n_status_paid, 0), 3) AS cnt_per_n_status_paid,
        LEAST(cnt, n_status_paid) AS least_cnt_n_status_paid
    FROM screened
)
SELECT DISTINCT least_cnt_n_status_paid, cnt_per_n_status_paid, claim_date_ym
FROM labeled
ORDER BY claim_date_ym DESC

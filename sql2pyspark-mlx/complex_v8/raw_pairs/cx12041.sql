WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_approved_amount
    FROM scoped_claims s
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(claim_date_quarter) AS claim_date_quarter_day,
        MAX(claimed_amount) AS max_claimed_amount,
        ROUND(SUM(claimed_amount_per_approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS claimed_amount_per_approved_amount_to_claimed_am,
        SUM(CASE WHEN claim_type <> 'theft' THEN claimed_amount_per_approved_amount ELSE 0 END) AS claimed_amount_per_approved_amount_claim_type_th,
        MAX(claim_date_quarter) AS last_claim_date_quarter,
        ROUND(SUM(min_claim_approved_amount), 2) AS total_min_claim_approved_amount,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM summarized_rows
    GROUP BY TO_DATE(claim_date_quarter)
    HAVING AVG(claim_count) > 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        claim_date_quarter_day,
        max_claimed_amount,
        claimed_amount_per_approved_amount_to_claimed_am,
        claimed_amount_per_approved_amount_claim_type_th,
        total_min_claim_approved_amount,
        claim_type_list,
        ROUND(CUME_DIST() OVER w_last_claim_date_quarter_year, 3) AS total_min_claim_approved_amount_cume_dist,
        ROUND(PERCENT_RANK() OVER w_all_by_max_claimed_amount_ti, 3) AS max_claimed_amount_percent_rank,
        MAX(max_claimed_amount) OVER (PARTITION BY YEAR(last_claim_date_quarter) ORDER BY total_min_claim_approved_amount, claim_date_quarter_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_max_claimed_amount_so_far,
        ROUND(AVG(max_claimed_amount) OVER (ORDER BY max_claimed_amount DESC, claim_date_quarter_day ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS max_claimed_amount_avg_6_prior,
        max_claimed_amount - LAG(max_claimed_amount) OVER w_last_claim_date_quarter_year_3 AS max_claimed_amount_change,
        NTH_VALUE(claim_date_quarter_day, 2) OVER (ORDER BY max_claimed_amount DESC, claim_date_quarter_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date_quarter_day
    FROM stats
    WINDOW w_last_claim_date_quarter_year AS (PARTITION BY YEAR(last_claim_date_quarter) ORDER BY total_min_claim_approved_amount), w_all_by_max_claimed_amount_ti AS (ORDER BY max_claimed_amount DESC), w_last_claim_date_quarter_year_3 AS (PARTITION BY YEAR(last_claim_date_quarter) ORDER BY total_min_claim_approved_amount, claim_date_quarter_day)
),
annotated AS (
    -- Add calculated columns
    SELECT
        claim_date_quarter_day,
        max_claimed_amount,
        claimed_amount_per_approved_amount_to_claimed_am,
        claimed_amount_per_approved_amount_claim_type_th,
        total_min_claim_approved_amount,
        total_min_claim_approved_amount_cume_dist,
        max_claimed_amount_percent_rank,
        max_max_claimed_amount_so_far,
        max_claimed_amount_avg_6_prior,
        max_claimed_amount_change,
        second_claim_date_quarter_day,
        ADD_MONTHS(claim_date_quarter_day, 3) AS claim_date_quarter_day_plus_3m,
        DATE_FORMAT(claim_date_quarter_day, 'yyyy-MM-dd') AS claim_date_quarter_day_label,
        ROUND(max_claimed_amount * 1.1, 2) AS max_claimed_amount_adj
    FROM metrics
),
labeled AS (
    -- Add calculated columns
    SELECT
        claim_date_quarter_day,
        max_claimed_amount,
        claimed_amount_per_approved_amount_to_claimed_am,
        claimed_amount_per_approved_amount_claim_type_th,
        total_min_claim_approved_amount,
        total_min_claim_approved_amount_cume_dist,
        second_claim_date_quarter_day,
        claim_date_quarter_day_plus_3m,
        claim_date_quarter_day_label,
        max_claimed_amount_adj,
        CASE WHEN max_claimed_amount >= 37525.0 THEN 'large' WHEN max_claimed_amount >= 12575.0 THEN 'medium' ELSE 'small' END AS max_claimed_amount_tier,
        DATE_TRUNC('WEEK', claim_date_quarter_day) AS claim_date_quarter_day_week
    FROM annotated
)
-- Final projection
SELECT
    claim_date_quarter_day,
    max_claimed_amount,
    claimed_amount_per_approved_amount_to_claimed_am,
    total_min_claim_approved_amount,
    total_min_claim_approved_amount_cume_dist,
    second_claim_date_quarter_day,
    claim_date_quarter_day_plus_3m,
    claim_date_quarter_day_label,
    max_claimed_amount_tier,
    claim_date_quarter_day_week,
    CASE WHEN max_claimed_amount >= 42515.0 THEN 'HIGH' WHEN max_claimed_amount >= 22555.0 THEN 'MEDIUM' ELSE 'LOW' END AS max_claimed_amount_tier_2,
    CASE WHEN max_claimed_amount >= 37525.0 THEN 'HIGH' WHEN max_claimed_amount >= 17565.0 THEN 'MEDIUM' ELSE 'LOW' END AS max_claimed_amount_tier_3
FROM labeled
ORDER BY claim_date_quarter_day_week, second_claim_date_quarter_day

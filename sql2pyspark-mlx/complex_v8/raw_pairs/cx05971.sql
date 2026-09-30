WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE claimed_amount > 11235.6
      AND adjuster_id <= 6
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY adjuster_id, status ORDER BY greatest_approved_amount_claimed_amount ASC, id)) AS days_since_prev_claim_date,
        NTILE(10) OVER (PARTITION BY status ORDER BY claim_date DESC, id) AS claim_date_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY claim_date DESC), 3) AS claim_date_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY claim_date DESC, id) AS seq_no,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, status ORDER BY greatest_approved_amount_claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        LAST_VALUE(claim_date) OVER (PARTITION BY adjuster_id, status ORDER BY greatest_approved_amount_claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY greatest_approved_amount_claimed_amount DESC, id), 3) AS greatest_approved_amount_claimed_amount_cume_dis
    FROM valid_claims
),
top_rows AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        status_ref,
        greatest_approved_amount_claimed_amount,
        days_since_prev_claim_date,
        claim_date_decile,
        claim_date_cume_dist,
        last_known_claim_date,
        last_claim_date,
        greatest_approved_amount_claimed_amount_cume_dis
    FROM metrics
    WHERE seq_no <= 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(days_since_prev_claim_date) OVER (PARTITION BY claim_type, YEAR(last_claim_date) ORDER BY last_claim_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_since_prev_claim_date_avg_4,
        RANK() OVER (PARTITION BY claim_type, YEAR(last_claim_date) ORDER BY last_claim_date NULLS LAST, id) AS last_claim_date_rank,
        ROUND(claim_date_decile - AVG(claim_date_decile) OVER (PARTITION BY claim_type, claim_date_decile), 2) AS claim_date_decile_vs_claim_type_claim_date_decil,
        MAX(claim_date_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), status ORDER BY days_since_prev_claim_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claim_date_cume_dist_max_1_prior,
        ROUND(AVG(claim_date_decile) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), status ORDER BY days_since_prev_claim_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS claim_date_decile_avg_1_prior,
        DENSE_RANK() OVER (PARTITION BY YEAR(last_claim_date), adjuster_id ORDER BY last_claim_date ASC, id) AS last_claim_date_dense_rank,
        ROUND(claim_date_cume_dist / NULLIF(SUM(claim_date_cume_dist) OVER (PARTITION BY claim_type, claim_date_decile), 0), 4) AS claim_date_cume_dist_share_of_claim_type_claim_d,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY claim_type, claim_date_decile)) AS distinct_policy_id_in_claim_type_claim_date_deci
    FROM top_rows
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_date_decile,
        YEAR(claim_date) AS claim_date_year,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        COUNT(*) AS cnt
    FROM with_windows
    GROUP BY claim_date_decile, YEAR(claim_date)
    HAVING AVG(claim_date_decile) > 4
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        claim_date_decile,
        claim_date_year,
        claim_date_span_days,
        cnt,
        ROUND(AVG(claim_date_decile) OVER (PARTITION BY claim_date_year, cnt), 2) AS claim_date_year_cnt_avg_claim_date_decile,
        COUNT(*) OVER w_claim_date_year_cnt AS rows_in_claim_date_year_cnt,
        ROUND(claim_date_span_days / NULLIF(SUM(claim_date_span_days) OVER (PARTITION BY claim_date_year, cnt), 0), 4) AS claim_date_span_days_share_of_claim_date_year_cn,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date_span_days DESC), 3) AS claim_date_span_days_cume_dist
    FROM per_group
    WINDOW w_claim_date_year_cnt AS (PARTITION BY claim_date_year, cnt)
)
SELECT
    claim_date_decile,
    claim_date_year,
    cnt,
    claim_date_year_cnt_avg_claim_date_decile,
    rows_in_claim_date_year_cnt,
    claim_date_span_days_cume_dist,
    claim_date_span_days + claim_date_decile AS claim_date_span_days_plus_claim_date_decile,
    COALESCE(claim_date_year, 0) AS claim_date_year_filled
FROM ranked

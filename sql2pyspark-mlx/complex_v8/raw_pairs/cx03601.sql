WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claimed_amount >= 19609.8
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        claim_type_len,
        least_claimed_amount_approved_amount,
        greatest_claimed_amount_approved_amount,
        claimed_amount - LAG(claimed_amount) OVER (ORDER BY least_claimed_amount_approved_amount, id) AS claimed_amount_change,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_least_claimed_amount) AS days_since_prev_claim_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        MIN(approved_amount) OVER w_policy_id AS policy_id_min_approved_amount
    FROM valid
    WINDOW w_all_by_least_claimed_amount AS (ORDER BY least_claimed_amount_approved_amount, id), w_policy_id AS (PARTITION BY policy_id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(claim_type_len - AVG(claim_type_len) OVER w_claim_type_len, 2) AS claim_type_len_vs_claim_type_len_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_least_claimed_amount, 3) AS least_claimed_amount_approved_amount_percent_ran
    FROM with_trends
    WINDOW w_claim_type_len AS (PARTITION BY claim_type_len), w_all_by_least_claimed_amount AS (ORDER BY least_claimed_amount_approved_amount, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        ROUND(SUM(least_claimed_amount_approved_amount_percent_ran) / NULLIF(SUM(claim_type_len), 0), 3) AS least_claimed_amount_approved_amount_percent_ran_2,
        ROUND(SUM(greatest_claimed_amount_approved_amount) / NULLIF(SUM(policy_id_min_approved_amount), 0), 3) AS greatest_claimed_amount_approved_amount_to_polic,
        MAX(claim_type_len_vs_claim_type_len_avg) AS max_claim_type_len_vs_claim_type_len_avg,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) AS row_count
    FROM windowed
    WHERE approved_amount <= 23120.7
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
tagged AS (
    -- Add calculated columns
    SELECT
        status,
        claim_date_ym,
        max_claim_type_len_vs_claim_type_len_avg,
        row_count,
        CONCAT_WS('|', claim_date_ym, status) AS claim_date_ym_status_label
    FROM summary
)
-- Final projection
SELECT
    status,
    claim_date_ym,
    max_claim_type_len_vs_claim_type_len_avg,
    row_count,
    SUBSTRING(TRIM(status), 1, 3) AS status_prefix
FROM tagged
ORDER BY claim_date_ym DESC, row_count DESC

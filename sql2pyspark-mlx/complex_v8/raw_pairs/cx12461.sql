WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ADD_MONTHS(claim_date, 1) AS claim_date_plus_1m
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY approved_amount DESC NULLS FIRST, id) AS approved_amount_seq,
        COUNT(*) OVER (PARTITION BY claim_type, MONTH(claim_date_plus_1m) ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY claim_type, MONTH(claim_date_plus_1m) ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        MAX(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_max_approved_amount,
        LAST_VALUE(claim_date_plus_1m) IGNORE NULLS OVER (PARTITION BY policy_id, status ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date_plus_1m,
        ROW_NUMBER() OVER (PARTITION BY claim_type, MONTH(claim_date_plus_1m) ORDER BY approved_amount ASC, id) AS approved_amount_seq_2,
        SUM(approved_amount) OVER (PARTITION BY claim_type, MONTH(claim_date_plus_1m) ORDER BY approved_amount ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        MIN(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_min_claimed_amount,
        ROUND(CUME_DIST() OVER w_policy_id_status_by_claim_da_2, 3) AS claim_date_cume_dist,
        ROUND(AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_approved_amount,
        LEAD(approved_amount) OVER (PARTITION BY claim_type, MONTH(claim_date_plus_1m) ORDER BY approved_amount ASC, id) AS next_approved_amount,
        MIN(approved_amount) OVER (PARTITION BY claim_type) AS claim_type_min_approved_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type, status ORDER BY approved_amount DESC NULLS FIRST, id), 3) AS approved_amount_percent_rank
    FROM recent_claims
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_policy_id_status_by_claim_da_2 AS (PARTITION BY policy_id, status ORDER BY claim_date DESC NULLS FIRST)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE approved_amount_seq <= 2
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        ROUND(SUM(approved_amount_seq) / NULLIF(SUM(rows_in_claim_type), 0), 3) AS approved_amount_seq_to_rows_in_claim_type_ratio,
        ROUND(100 * COUNT(CASE WHEN claim_type = 'collision' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_collision
    FROM top_per_group
    GROUP BY DATE_TRUNC('MONTH', claim_date)
    HAVING COUNT(*) >= 3
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_row_count_claim_date_month_m AS rows_in_row_count_claim_date_month_month,
        LAST_VALUE(row_count) IGNORE NULLS OVER (PARTITION BY row_count ORDER BY row_count ASC, claim_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_count
    FROM summary
    WINDOW w_row_count_claim_date_month_m AS (PARTITION BY row_count, DATE_TRUNC('MONTH', claim_date_month))
)
SELECT
    claim_date_month,
    row_count,
    claim_type_list,
    approved_amount_seq_to_rows_in_claim_type_ratio,
    pct_claim_type_collision,
    rows_in_row_count_claim_date_month_month,
    last_known_row_count,
    MONTH(claim_date_month) AS claim_date_month_month,
    CASE WHEN last_known_row_count >= 10 THEN 'A' WHEN last_known_row_count >= 5 THEN 'B' ELSE 'C' END AS last_known_row_count_tier
FROM metrics

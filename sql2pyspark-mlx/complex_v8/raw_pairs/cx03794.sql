WITH base_claims AS (
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
        LAST_DAY(claim_date) AS claim_date_month_end,
        LAST_DAY(claim_date) AS claim_date_month_end_2
    FROM claims
    WHERE approved_amount < 23487.5
      AND adjuster_id >= 4
),
unique_rows AS (
    -- Keep the latest row per adjuster_id
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
        claim_date_month_end,
        claim_date_month_end_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id DESC) AS row_num
        FROM base_claims
    ) ranked
    WHERE row_num = 1
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM unique_rows s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id
    FROM summarized_rows s
    JOIN policies p ON p.id = s.policy_id
),
totals AS (
    -- Summarise per group
    SELECT
        policy_id,
        DATE_FORMAT(policy_end_date, 'yyyy-MM') AS policy_end_date_ym,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_ym,
        ROUND(SUM(claimed_amount), 2) AS total_claimed_amount,
        DATEDIFF(MAX(policy_end_date), MIN(policy_end_date)) AS policy_end_date_span_days,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        SUM(CASE WHEN claim_type IN ('fire', 'theft') THEN claim_count ELSE 0 END) AS claim_count_claim_type_in_fire_theft
    FROM enriched_policy
    WHERE adjuster_id >= 3
    GROUP BY policy_id, DATE_FORMAT(policy_end_date, 'yyyy-MM'), DATE_FORMAT(reported_at, 'yyyy-MM')
),
with_details AS (
    -- Join policies
    SELECT
        s.policy_id,
        s.policy_end_date_ym,
        s.reported_at_ym,
        s.total_claimed_amount,
        s.policy_end_date_span_days,
        s.status_list,
        s.claim_count_claim_type_in_fire_theft,
        p.status AS policy_status,
        p.product AS policy_product,
        p.start_date AS policy_start_date
    FROM totals s
    LEFT JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        policy_id,
        policy_end_date_ym,
        reported_at_ym,
        total_claimed_amount,
        policy_end_date_span_days,
        status_list,
        MIN(total_claimed_amount) OVER w_policy_product_policy_start AS policy_product_policy_start_date_year_min_total_,
        DENSE_RANK() OVER (ORDER BY claim_count_claim_type_in_fire_theft DESC) AS claim_count_claim_type_in_fire_theft_dense_rank,
        LAST_VALUE(policy_start_date) IGNORE NULLS OVER (PARTITION BY policy_status, policy_product ORDER BY policy_end_date_span_days, policy_id, policy_end_date_ym, reported_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_policy_start_date
    FROM with_details
    WINDOW w_policy_product_policy_start AS (PARTITION BY policy_product, YEAR(policy_start_date))
)
-- Subtotals with CUBE
SELECT
    claim_count_claim_type_in_fire_theft_dense_rank,
    policy_id,
    ROUND(STDDEV(policy_end_date_span_days), 2) AS stddev_policy_end_date_span_days,
    MIN(policy_end_date_span_days) AS min_policy_end_date_span_days
FROM metrics
GROUP BY CUBE(claim_count_claim_type_in_fire_theft_dense_rank, policy_id)

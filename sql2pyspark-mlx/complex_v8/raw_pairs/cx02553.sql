WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date
    FROM claims
    WHERE approved_amount >= 4234.9
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        s.days_since_claim_date,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM prepared_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at,
        days_since_claim_date,
        policy_holder_id,
        policy_product,
        COUNT(*) OVER (PARTITION BY policy_product) AS rows_in_policy_product,
        ROUND(policy_premium / NULLIF(SUM(policy_premium) OVER w_policy_product, 0), 4) AS policy_premium_share_of_policy_product,
        SUM(policy_premium) OVER (ORDER BY days_since_claim_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS policy_premium_sum_2,
        LAST_VALUE(reported_at) OVER (PARTITION BY adjuster_id ORDER BY policy_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        LEAD(reported_at) OVER (ORDER BY days_since_claim_date DESC, id) AS next_reported_at,
        MAX(days_since_claim_date) OVER (PARTITION BY policy_product) AS policy_product_max_days_since_claim_date,
        NTILE(4) OVER (ORDER BY days_since_claim_date DESC, id) AS days_since_claim_date_quartile,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY policy_product)) AS distinct_policy_id_in_policy_product
    FROM joined
    WINDOW w_policy_product AS (PARTITION BY policy_product)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        reported_at,
        days_since_claim_date,
        policy_holder_id,
        rows_in_policy_product,
        policy_premium_share_of_policy_product,
        policy_premium_sum_2,
        last_reported_at,
        next_reported_at,
        policy_product_max_days_since_claim_date,
        days_since_claim_date_quartile,
        CONCAT(policy_product, '#', CAST(policy_id AS STRING)) AS policy_product_ref,
        SUBSTRING(TRIM(policy_product), 1, 2) AS policy_product_prefix,
        policy_premium_sum_2 - days_since_claim_date AS policy_premium_sum_2_minus_days_since_claim_date
    FROM ranked
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER w_all_by_days_since_claim_date_3 AS days_since_claim_date_quartile_quintile,
        MIN(rows_in_policy_product) OVER w_adjuster_id_policy_holder_id AS adjuster_id_policy_holder_id_min_rows_in_policy_,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY YEAR(last_reported_at), policy_holder_id ORDER BY last_reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        LAG(days_since_claim_date) OVER (PARTITION BY YEAR(last_reported_at), policy_holder_id ORDER BY last_reported_at, id) AS prev_days_since_claim_date,
        RANK() OVER (PARTITION BY MONTH(next_reported_at) ORDER BY rows_in_policy_product NULLS LAST) AS rows_in_policy_product_rank,
        ROUND(AVG(rows_in_policy_product) OVER (ORDER BY next_reported_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rows_in_policy_product_avg_7,
        LAG(days_since_claim_date, 1, 0) OVER w_next_reported_at_month_by_ro_2 AS prev_days_since_claim_date_2
    FROM scored
    WINDOW w_all_by_days_since_claim_date_3 AS (ORDER BY days_since_claim_date_quartile, id), w_adjuster_id_policy_holder_id AS (PARTITION BY adjuster_id, policy_holder_id), w_next_reported_at_month_by_ro_2 AS (PARTITION BY MONTH(next_reported_at) ORDER BY rows_in_policy_product NULLS LAST, id)
),
summary AS (
    -- Summarise per group
    SELECT
        days_since_claim_date_quartile,
        rows_in_policy_product_rank,
        ROUND(AVG(rows_in_policy_product_avg_7), 2) AS avg_rows_in_policy_product_avg_7,
        MIN(prev_days_since_claim_date) AS min_prev_days_since_claim_date
    FROM ranked_2
    GROUP BY days_since_claim_date_quartile, rows_in_policy_product_rank
)
SELECT
    days_since_claim_date_quartile,
    rows_in_policy_product_rank,
    min_prev_days_since_claim_date
FROM summary
ORDER BY days_since_claim_date_quartile DESC, rows_in_policy_product_rank DESC

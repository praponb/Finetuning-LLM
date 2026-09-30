WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id)) AS distinct_product_in_holder_id,
        LAST_VALUE(end_date) OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        MAX(premium) OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_max_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id), 3) AS end_date_percent_rank
    FROM recent_policys
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count_2
    FROM with_windows s
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.distinct_product_in_holder_id,
        s.last_end_date,
        s.premium_max_3_prior,
        s.end_date_percent_rank,
        s.claim_count,
        p.segment AS holder_segment
    FROM summarized_rows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY holder_segment ORDER BY start_date DESC, id) AS start_date_seq,
        SUM(end_date_percent_rank) OVER (PARTITION BY holder_segment ORDER BY start_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS end_date_percent_rank_sum_4,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        NTILE(4) OVER (PARTITION BY holder_segment ORDER BY start_date DESC, id) AS start_date_quartile
    FROM combined_details
)
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    distinct_product_in_holder_id,
    last_end_date,
    premium_max_3_prior,
    end_date_percent_rank,
    claim_count,
    holder_segment,
    end_date_percent_rank_sum_4,
    rows_in_holder_id,
    INITCAP(TRIM(status)) AS status_clean,
    ROUND(end_date_percent_rank * 0.25, 2) AS end_date_percent_rank_adj
FROM with_trends

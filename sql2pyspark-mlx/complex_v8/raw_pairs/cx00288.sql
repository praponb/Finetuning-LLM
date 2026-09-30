WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LENGTH(TRIM(product)) AS product_len,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE holder_id >= 13
      AND end_date BETWEEN DATE '2026-01-20' AND DATE '2026-09-19'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE claim_type = 'medical') AS claim_claim_type_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM filtered_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        ROUND(SUM(premium) / NULLIF(SUM(premium_filled), 0), 3) AS premium_to_premium_filled_ratio,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(DISTINCT product) AS distinct_products,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN product <> 'TRAVEL' THEN 1 END) / COUNT(*), 1) AS pct_product_travel
    FROM summarized_rows
    WHERE start_date >= DATE '2024-05-07'
    GROUP BY DATE_TRUNC('MONTH', end_date)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(distinct_holder_ids) IGNORE NULLS OVER (PARTITION BY distinct_holder_ids, distinct_products ORDER BY distinct_products DESC, end_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_holder_ids,
        COUNT(*) OVER w_end_date_month_month AS rows_in_end_date_month_month,
        DENSE_RANK() OVER (PARTITION BY cnt ORDER BY distinct_holder_ids DESC) AS distinct_holder_ids_dense_rank
    FROM per_group
    WINDOW w_end_date_month_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date_month))
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(end_date_month) OVER (PARTITION BY distinct_holder_ids_dense_rank, distinct_products ORDER BY distinct_holder_ids_dense_rank DESC, end_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date_month,
        FIRST_VALUE(distinct_products) IGNORE NULLS OVER (PARTITION BY distinct_holder_ids_dense_rank, distinct_products ORDER BY distinct_holder_ids_dense_rank DESC, end_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_products,
        cnt - LAG(cnt) OVER (PARTITION BY distinct_holder_ids_dense_rank, distinct_products ORDER BY distinct_holder_ids_dense_rank DESC, end_date_month) AS cnt_change,
        ROUND(distinct_holder_ids - AVG(distinct_holder_ids) OVER (PARTITION BY cnt), 2) AS distinct_holder_ids_vs_cnt_avg
    FROM with_windows
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        end_date_month,
        premium_to_premium_filled_ratio,
        distinct_holder_ids,
        cnt,
        pct_product_travel,
        last_known_distinct_holder_ids,
        rows_in_end_date_month_month,
        last_end_date_month,
        first_known_distinct_products,
        cnt_change,
        distinct_holder_ids_vs_cnt_avg,
        SUM(rows_in_end_date_month_month) OVER w_last_known_distinct_holder_i AS last_known_distinct_holder_ids_first_known_disti,
        LEAD(distinct_products) OVER (PARTITION BY cnt, distinct_holder_ids_dense_rank ORDER BY pct_product_travel DESC, end_date_month) AS next_distinct_products,
        SUM(cnt_change) OVER (PARTITION BY last_known_distinct_holder_ids ORDER BY distinct_holder_ids_vs_cnt_avg DESC, end_date_month ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cnt_change_sum_2_prior
    FROM windowed
    WINDOW w_last_known_distinct_holder_i AS (PARTITION BY last_known_distinct_holder_ids, first_known_distinct_products)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled,
        ROUND(distinct_holder_ids / NULLIF(cnt_change, 0), 3) AS distinct_holder_ids_per_cnt_change
    FROM with_trends
)
SELECT
    end_date_month,
    distinct_holder_ids,
    cnt,
    pct_product_travel,
    rows_in_end_date_month_month,
    first_known_distinct_products,
    cnt_change,
    distinct_holder_ids_vs_cnt_avg,
    last_known_distinct_holder_ids_first_known_disti,
    next_distinct_products,
    cnt_change_sum_2_prior,
    distinct_holder_ids_filled
FROM labeled
WHERE last_known_distinct_holder_ids > 4
ORDER BY pct_product_travel, end_date_month DESC NULLS FIRST
LIMIT 20

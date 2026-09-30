WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE product = 'HOME'
),
with_details AS (
    -- Attach holder attributes
    SELECT s.*, p.segment AS holder_segment, p.joined_on AS holder_joined_on
    FROM valid_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        SUM(premium) OVER (PARTITION BY holder_id) AS holder_id_sum_premium,
        SIZE(COLLECT_SET(holder_segment) OVER (PARTITION BY holder_id)) AS distinct_holder_segment_in_holder_id,
        COUNT(*) OVER (PARTITION BY holder_id ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_details
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY holder_segment, distinct_holder_segment_in_holder_id ORDER BY distinct_holder_segment_in_holder_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(PERCENT_RANK() OVER w_holder_segment_distinct_hold_2, 3) AS distinct_holder_segment_in_holder_id_percent_ran,
        DENSE_RANK() OVER (PARTITION BY holder_segment, distinct_holder_segment_in_holder_id ORDER BY distinct_holder_segment_in_holder_id DESC NULLS FIRST) AS distinct_holder_segment_in_holder_id_dense_rank,
        SIZE(COLLECT_SET(holder_segment) OVER w_product_distinct_holder_segm) AS distinct_holder_segment_in_product_distinct_hold
    FROM windowed
    WINDOW w_holder_segment_distinct_hold_2 AS (PARTITION BY holder_segment, distinct_holder_segment_in_holder_id ORDER BY distinct_holder_segment_in_holder_id DESC NULLS FIRST, id), w_product_distinct_holder_segm AS (PARTITION BY product, distinct_holder_segment_in_holder_id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        holder_segment,
        second_end_date,
        holder_id_sum_premium,
        running_count,
        running_premium,
        distinct_holder_segment_in_product_distinct_hold,
        CASE WHEN holder_segment IN ('RETAIL', 'SME') THEN 'group_a' WHEN holder_segment = 'VIP' THEN 'group_b' ELSE 'other' END AS holder_segment_group,
        DATE_TRUNC('YEAR', start_date) AS start_date_year
    FROM windowed_2
    WHERE holder_id_sum_premium >= 5180.0
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(premium) OVER w_product AS product_min_premium,
        ROUND(AVG(premium) OVER (PARTITION BY product), 2) AS product_avg_premium,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST) AS end_date_dense_rank,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROUND(holder_id_sum_premium / NULLIF(SUM(holder_id_sum_premium) OVER w_product, 0), 4) AS holder_id_sum_premium_share_of_product,
        LAG(start_date) OVER w_holder_id_by_end_date AS prev_start_date
    FROM derived
    WINDOW w_product AS (PARTITION BY product), w_holder_id_by_end_date AS (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    holder_segment,
    second_end_date,
    running_count,
    running_premium,
    distinct_holder_segment_in_product_distinct_hold,
    end_date_dense_rank,
    rows_in_product,
    prev_start_date
FROM metrics
WHERE second_end_date BETWEEN DATE '2025-09-27' AND DATE '2027-03-20'
ORDER BY second_end_date, rows_in_product

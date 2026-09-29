WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        CASE WHEN premium >= 4417.0 THEN 'A' WHEN premium >= 2096.6 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE end_date >= DATE '2025-12-05'
      AND premium >= 2033.8
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        premium,
        end_date,
        status,
        premium_tier,
        ROUND(AVG(premium) OVER (ORDER BY status_len NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        SIZE(COLLECT_SET(premium_tier) OVER w_status_holder_id) AS distinct_premium_tier_in_status_holder_id,
        NTH_VALUE(status_len, 2) OVER (PARTITION BY premium_tier ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len,
        ROUND(PERCENT_RANK() OVER w_premium_tier_status_by_statu, 3) AS status_len_percent_rank,
        NTILE(5) OVER (ORDER BY status_len NULLS LAST, id) AS status_len_quintile,
        LAST_VALUE(premium) OVER (PARTITION BY premium_tier ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        LAG(premium, 3) OVER w_all_by_status_len AS prev3_premium,
        COUNT(*) OVER w_status_holder_id AS rows_in_status_holder_id,
        ROW_NUMBER() OVER w_premium_tier_by_premium AS seq_no,
        ROUND(AVG(premium) OVER w_product_status, 2) AS product_status_avg_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, status ORDER BY premium DESC), 3) AS premium_cume_dist,
        FIRST_VALUE(status_len) OVER (PARTITION BY premium_tier, status ORDER BY status_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_status_len
    FROM cleaned_policys
    WINDOW w_status_holder_id AS (PARTITION BY status, holder_id), w_premium_tier_status_by_statu AS (PARTITION BY premium_tier, status ORDER BY status_len, id), w_all_by_status_len AS (ORDER BY status_len NULLS LAST, id), w_premium_tier_by_premium AS (PARTITION BY premium_tier ORDER BY premium, id), w_product_status AS (PARTITION BY product, status)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        end_date,
        second_status_len,
        status_len_percent_rank,
        status_len_quintile,
        last_premium,
        prev3_premium,
        seq_no,
        premium_cume_dist,
        first_status_len,
        ROUND(premium_cume_dist / NULLIF(premium_avg_4, 0), 3) AS premium_cume_dist_per_premium_avg_4,
        second_status_len - first_status_len AS second_status_len_minus_first_status_len
    FROM with_trends
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        premium,
        end_date,
        status_len_quintile,
        prev3_premium,
        seq_no,
        premium_cume_dist,
        first_status_len,
        premium_cume_dist_per_premium_avg_4,
        second_status_len_minus_first_status_len,
        DENSE_RANK() OVER (PARTITION BY status_len_quintile, seq_no ORDER BY prev3_premium ASC) AS prev3_premium_dense_rank,
        DENSE_RANK() OVER (ORDER BY last_premium ASC, id) AS last_premium_dense_rank,
        NTH_VALUE(prev3_premium, 2) OVER (ORDER BY last_premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev3_premium
    FROM enriched
)
SELECT
    id,
    premium,
    end_date,
    status_len_quintile,
    prev3_premium,
    premium_cume_dist,
    first_status_len,
    premium_cume_dist_per_premium_avg_4,
    second_status_len_minus_first_status_len,
    prev3_premium_dense_rank,
    LEAST(status_len_quintile, premium) AS least_status_len_quintile_premium,
    premium_cume_dist + seq_no AS premium_cume_dist_plus_seq_no
FROM metrics
WHERE premium_cume_dist_per_premium_avg_4 >= 4.5
ORDER BY premium_cume_dist DESC, premium DESC, id
LIMIT 10

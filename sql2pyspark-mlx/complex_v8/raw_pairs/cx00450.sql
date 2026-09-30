WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CASE WHEN status IN ('EXPIRED', 'CANCELLED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN premium >= 2844.1 THEN 'HIGH' WHEN premium >= 2033.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE holder_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.status_group,
        p.holder_name,
        p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on < DATE '2025-02-22'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        status_group,
        holder_segment,
        COUNT(*) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER w_holder_segment_status_group AS premium_seq,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status), 0), 4) AS premium_share_of_status,
        LAG(premium, 3) OVER (PARTITION BY holder_id ORDER BY premium DESC, id) AS prev3_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status_group), 2) AS premium_vs_status_group_avg,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, holder_segment), 2) AS premium_vs_product_holder_segment_avg,
        NTH_VALUE(premium, 2) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        NTH_VALUE(premium, 2) OVER (ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_2,
        ROW_NUMBER() OVER w_all_by_premium AS rn,
        LEAD(premium) OVER (PARTITION BY status, holder_segment ORDER BY premium DESC, id) AS next_premium,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status)) AS distinct_product_in_status,
        premium - LAG(premium) OVER (PARTITION BY holder_segment, status_group ORDER BY premium DESC, id) AS premium_change,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY premium DESC, id) AS premium_dense_rank,
        LEAD(premium, 2) OVER (ORDER BY premium, id) AS next2_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status_group), 0), 4) AS premium_share_of_status_group
    FROM combined_details
    WINDOW w_holder_segment_status_group AS (PARTITION BY holder_segment, status_group ORDER BY premium DESC, id), w_all_by_premium AS (ORDER BY premium DESC, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(running_count) OVER (PARTITION BY premium_seq, premium_dense_rank ORDER BY second_premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count,
        NTILE(4) OVER w_premium_dense_rank_holder_se AS second_premium_2_quartile,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status_group), 0), 4) AS premium_share_of_status_group_2,
        ROUND(premium_change / NULLIF(SUM(premium_change) OVER (PARTITION BY status_group), 0), 4) AS premium_change_share_of_status_group
    FROM windowed
    WINDOW w_premium_dense_rank_holder_se AS (PARTITION BY premium_dense_rank, holder_segment ORDER BY second_premium_2, id)
)
-- Final projection
SELECT
    id,
    status_group,
    running_count,
    premium_seq,
    prev3_premium,
    premium_vs_status_group_avg,
    premium_vs_product_holder_segment_avg,
    next_premium,
    premium_change,
    premium_dense_rank,
    next2_premium,
    premium_share_of_status_group,
    running_running_count,
    premium_share_of_status_group_2
FROM with_windows
WHERE premium_vs_status_group_avg < 3500.0
ORDER BY premium_dense_rank, running_count DESC

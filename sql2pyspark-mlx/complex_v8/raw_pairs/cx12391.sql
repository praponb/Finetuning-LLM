WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN premium >= 3465.4 THEN 'large' WHEN premium >= 1541.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE status = 'ACTIVE'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        premium_tier,
        LEAD(start_date) OVER (ORDER BY start_date, id) AS next_start_date,
        SUM(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_sum_2,
        ROUND(AVG(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_2_prior,
        ROUND(CUME_DIST() OVER (ORDER BY start_date), 3) AS start_date_cume_dist
    FROM prepared_policys
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        premium_tier,
        next_start_date,
        premium_sum_2,
        premium_avg_2_prior,
        start_date_cume_dist,
        MAX(start_date_cume_dist) OVER (PARTITION BY holder_id, product ORDER BY next_start_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS start_date_cume_dist_max_3_prior,
        ROUND(premium_avg_2_prior / NULLIF(SUM(premium_avg_2_prior) OVER w_premium_tier_status, 0), 4) AS premium_avg_2_prior_share_of_premium_tier_status,
        MIN(premium) OVER (PARTITION BY premium_tier, status) AS premium_tier_status_min_premium,
        NTH_VALUE(next_start_date, 2) OVER (PARTITION BY holder_id, product ORDER BY next_start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_start_date,
        RANK() OVER (ORDER BY premium_sum_2 DESC, id) AS premium_sum_2_rank
    FROM windowed
    WINDOW w_premium_tier_status AS (PARTITION BY premium_tier, status)
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium_tier,
        s.premium_sum_2,
        s.premium_avg_2_prior,
        s.start_date_cume_dist,
        s.start_date_cume_dist_max_3_prior,
        s.premium_avg_2_prior_share_of_premium_tier_status,
        s.premium_sum_2_rank,
        p.segment AS holder_segment
    FROM with_trends s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on < DATE '2024-12-21'
    WHERE p.segment = 'VIP'
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.status <> 'CANCELLED') FROM policies p WHERE p.holder_id = s.holder_id) AS policy_status_count,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium
    FROM with_parent_holder s
),
deduped AS (
    -- Keep the earliest row per premium_tier, holder_segment
    SELECT
        id,
        holder_id,
        product,
        premium_tier,
        premium_sum_2,
        premium_avg_2_prior,
        start_date_cume_dist,
        start_date_cume_dist_max_3_prior,
        premium_avg_2_prior_share_of_premium_tier_status,
        premium_sum_2_rank,
        holder_segment,
        policy_status_count,
        avg_policy_premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY premium_tier, holder_segment ORDER BY avg_policy_premium, id) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY premium_tier ORDER BY premium_avg_2_prior_share_of_premium_tier_status ASC), 3) AS premium_avg_2_prior_share_of_premium_tier_status_2,
        ROUND(premium_sum_2_rank - AVG(premium_sum_2_rank) OVER w_holder_segment_product, 2) AS premium_sum_2_rank_vs_holder_segment_product_avg,
        LAG(avg_policy_premium, 1, 0) OVER (ORDER BY avg_policy_premium DESC, id) AS prev_avg_policy_premium,
        FIRST_VALUE(premium_avg_2_prior) IGNORE NULLS OVER (PARTITION BY premium_tier ORDER BY premium_avg_2_prior_share_of_premium_tier_status ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_avg_2_prior,
        MAX(premium_sum_2) OVER (PARTITION BY holder_segment, product) AS holder_segment_product_max_premium_sum_2
    FROM deduped
    WINDOW w_holder_segment_product AS (PARTITION BY holder_segment, product)
)
SELECT
    id,
    holder_id,
    premium_tier,
    premium_sum_2,
    premium_avg_2_prior,
    start_date_cume_dist_max_3_prior,
    premium_avg_2_prior_share_of_premium_tier_status,
    premium_sum_2_rank,
    holder_segment,
    policy_status_count,
    avg_policy_premium,
    premium_avg_2_prior_share_of_premium_tier_status_2,
    premium_sum_2_rank_vs_holder_segment_product_avg,
    prev_avg_policy_premium,
    first_known_premium_avg_2_prior,
    holder_segment_product_max_premium_sum_2,
    COALESCE(avg_policy_premium, 0.0) AS avg_policy_premium_filled,
    LOWER(TRIM(holder_segment)) AS holder_segment_clean
FROM sequenced
ORDER BY premium_tier DESC, prev_avg_policy_premium, id
LIMIT 5

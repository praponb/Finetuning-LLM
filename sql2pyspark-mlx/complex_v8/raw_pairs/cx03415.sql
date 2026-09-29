WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 2289.0) AS premium_distance,
        CASE WHEN product IN ('TRAVEL', 'HEALTH') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE premium BETWEEN 2013.8 AND 3719.7
      AND holder_id <= 13
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(premium_distance / NULLIF(premium, 0), 3) AS premium_distance_per_premium,
        LENGTH(TRIM(product_group)) AS product_group_len,
        premium_distance + premium AS premium_distance_plus_premium
    FROM scoped
    WHERE premium <= 3532.7
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        TRUNC(start_date, 'MM') AS start_date_month_start,
        LENGTH(TRIM(status)) AS status_len
    FROM tagged
    WHERE premium <= 3465.4
),
with_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        s.product_group,
        s.premium_distance_per_premium,
        s.premium_distance_plus_premium,
        s.start_date_month_start,
        p.holder_name,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM derived s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium_distance_per_premium / NULLIF(SUM(premium_distance_per_premium) OVER w_holder_segment, 0), 4) AS premium_distance_per_premium_share_of_holder_seg,
        ROUND(AVG(premium_distance_per_premium) OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_distance_per_premium_avg_1_prior
    FROM with_details
    WINDOW w_holder_segment AS (PARTITION BY holder_segment)
),
summary AS (
    -- Group-level totals
    SELECT
        holder_region,
        MIN(end_date) AS first_end_date,
        SUM(CASE WHEN holder_segment = 'VIP' THEN premium_distance_per_premium ELSE 0 END) AS premium_distance_per_premium_holder_segment_vip,
        MIN(start_date) AS first_start_date,
        ROUND(SUM(premium_distance_per_premium_avg_1_prior) / NULLIF(SUM(premium_distance_per_premium_share_of_holder_seg), 0), 3) AS premium_distance_per_premium_avg_1_prior_to_prem,
        MIN(premium_distance_per_premium_avg_1_prior) AS min_premium_distance_per_premium_avg_1_prior
    FROM windowed
    GROUP BY 1
    HAVING MAX(premium_distance_plus_premium) > 2000.0
)
SELECT
    holder_region,
    first_end_date,
    premium_distance_per_premium_holder_segment_vip,
    first_start_date,
    premium_distance_per_premium_avg_1_prior_to_prem,
    min_premium_distance_per_premium_avg_1_prior,
    SUBSTRING(TRIM(holder_region), 1, 2) AS holder_region_prefix,
    COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown
FROM summary
ORDER BY holder_region_or_unknown

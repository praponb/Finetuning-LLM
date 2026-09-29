WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        CASE WHEN product IN ('LIFE', 'TRAVEL') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE holder_id <= 7
      AND start_date IS NOT NULL
),
product_group_stats AS (
    -- Max premium per product_group
    SELECT product_group, MAX(premium) AS max_premium
    FROM prepared_policys
    GROUP BY product_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM prepared_policys s
    JOIN product_group_stats g ON g.product_group = s.product_group
    WHERE s.premium = g.max_premium
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(start_date) AS first_policy_start_date,
        COUNT(CASE WHEN product IS NOT NULL THEN 1 END) AS policy_product_count,
        COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_start_date,
        COALESCE(p.policy_product_count, 0) AS policy_product_count,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM vs_group s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY policy_product_count, YEAR(start_date)) AS policy_product_count_start_date_year_max_premium,
        NTILE(10) OVER w_start_date_year_by_max_premi AS max_premium_decile
    FROM with_counts
    WINDOW w_start_date_year_by_max_premi AS (PARTITION BY YEAR(start_date) ORDER BY max_premium NULLS LAST, id)
),
latest_per_key AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        product_group,
        max_premium,
        premium_minus_max,
        first_policy_start_date,
        policy_product_count,
        policy_count,
        policy_product_count_start_date_year_max_premium,
        max_premium_decile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY max_premium DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    status,
    product_group,
    max_premium,
    premium_minus_max,
    policy_product_count,
    policy_product_count_start_date_year_max_premium
FROM latest_per_key
ORDER BY id DESC NULLS FIRST, premium_minus_max

WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE end_date >= DATE '2026-02-16'
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'CANCELLED'
              AND p.product <> 'TRAVEL'
        )
),
totals AS (
    -- Group-level totals
    SELECT
        holder_id,
        product,
        SUM(premium) FILTER (WHERE product = 'HEALTH') AS premium_product_health,
        SUM(premium) FILTER (WHERE product NOT IN ('HOME', 'HEALTH')) AS premium_product_not_in_home_health,
        ROUND(SUM(premium), 2) AS total_premium,
        MIN(start_date) AS first_start_date,
        COUNT(*) AS cnt
    FROM without_matches
    WHERE premium > 2111.3
    GROUP BY holder_id, product
),
exploded_detail AS (
    -- One row per policy
    SELECT
        s.holder_id,
        s.product,
        s.premium_product_not_in_home_health,
        s.total_premium,
        s.first_start_date,
        s.cnt,
        p.id AS policy_id,
        p.product AS policy_product,
        p.premium AS policy_premium
    FROM totals s
    JOIN policies p ON p.holder_id = s.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY product), 0), 4) AS cnt_share_of_product,
        SIZE(COLLECT_SET(policy_product) OVER (PARTITION BY product)) AS distinct_policy_product_in_product,
        ROUND(total_premium / NULLIF(SUM(total_premium) OVER w_product, 0), 4) AS total_premium_share_of_product,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product)) AS distinct_holder_id_in_product
    FROM exploded_detail
    WINDOW w_product AS (PARTITION BY product)
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date < DATE '2025-03-24'
        )
)
SELECT
    holder_id,
    premium_product_not_in_home_health,
    total_premium,
    first_start_date,
    cnt,
    policy_id,
    policy_product,
    policy_premium,
    cnt_share_of_product,
    distinct_policy_product_in_product,
    total_premium_share_of_product,
    distinct_holder_id_in_product,
    CONCAT_WS('-', product, policy_product) AS product_policy_product_label,
    LENGTH(TRIM(product)) AS product_len
FROM with_activity
WHERE total_premium < 19570.0
ORDER BY cnt, policy_id DESC

WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('LAPSED', 'CANCELLED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        ABS(premium - 3532.7) AS premium_distance,
        ABS(premium - 2945.9) AS premium_distance_2
    FROM policies
    WHERE product = 'HOME'
      AND status = 'LAPSED'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY status, product ORDER BY premium_distance_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        MAX(premium_distance_2) OVER (PARTITION BY holder_id, product) AS holder_id_product_max_premium_distance_2,
        FIRST_VALUE(premium_distance_2) OVER (PARTITION BY status, product ORDER BY premium_distance_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_distance_2,
        LAST_VALUE(end_date) OVER (PARTITION BY status, product ORDER BY premium_distance_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        LAST_VALUE(premium_distance_2) IGNORE NULLS OVER (PARTITION BY status, product ORDER BY premium_distance_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_distance_2,
        LAG(premium_distance, 2) OVER (PARTITION BY status, product ORDER BY premium_distance_2 DESC, id) AS prev2_premium_distance
    FROM filtered
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE last_known_premium_distance_2 > (SELECT AVG(last_known_premium_distance_2) FROM windowed)
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.premium_distance_2,
        s.running_premium,
        s.holder_id_product_max_premium_distance_2,
        s.first_premium_distance_2,
        s.last_end_date,
        s.last_known_premium_distance_2,
        s.prev2_premium_distance,
        p.holder_name
    FROM with_activity s
    JOIN policyholders p ON p.id = s.holder_id AND p.segment = 'VIP'
    WHERE LOWER(p.holder_name) LIKE '%us%'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        LEAST(premium_distance_2, running_premium) AS least_premium_distance_2_running_premium,
        YEAR(last_end_date) AS last_end_date_year
    FROM combined_details_holder
),
filtered_2 AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.risk_score > 0.2
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        premium_distance_2,
        running_premium,
        first_premium_distance_2,
        last_end_date,
        last_known_premium_distance_2,
        prev2_premium_distance,
        holder_name,
        days_since_start_date,
        least_premium_distance_2_running_premium,
        last_end_date_year,
        SUBSTRING(TRIM(holder_name), 1, 4) AS holder_name_prefix,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        DATE_ADD(last_end_date, 365) AS last_end_date_plus_365d
    FROM filtered_2
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    premium_distance_2,
    last_known_premium_distance_2,
    prev2_premium_distance,
    holder_name,
    least_premium_distance_2_running_premium,
    last_end_date_year,
    holder_name_prefix,
    product_prefix,
    last_end_date_plus_365d,
    LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
FROM enriched
WHERE prev2_premium_distance > 1250.0
ORDER BY last_end_date_year NULLS LAST

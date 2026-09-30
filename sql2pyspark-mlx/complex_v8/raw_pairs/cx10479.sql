WITH base AS (
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
    WHERE premium > 2289.0
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.holder_name,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on < DATE '2024-12-21'
    WHERE p.region IN ('Central', 'North')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(premium) OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        NTH_VALUE(holder_joined_on, 2) OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_joined_on,
        LAST_VALUE(holder_joined_on) OVER (PARTITION BY status ORDER BY holder_joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_joined_on,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on) ORDER BY premium ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        MIN(premium) OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        COUNT(*) OVER w_status_product AS rows_in_status_product,
        MAX(premium) OVER (PARTITION BY status ORDER BY holder_joined_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_max_4,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, product)) AS distinct_holder_id_in_status_product,
        LAST_VALUE(holder_joined_on) OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_joined_on_3
    FROM with_details_holder
    WINDOW w_status_product AS (PARTITION BY status, product)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    status,
    holder_name,
    holder_region,
    last_premium,
    second_holder_joined_on,
    last_holder_joined_on,
    min_premium_so_far,
    rows_in_status_product,
    distinct_holder_id_in_status_product,
    last_holder_joined_on_3,
    YEAR(end_date) AS end_date_year,
    LOWER(REGEXP_REPLACE(TRIM(holder_region), ' ', '_')) AS holder_region_slug
FROM windowed

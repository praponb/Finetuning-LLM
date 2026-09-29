WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        DATE_ADD(start_date, 30) AS start_date_plus_30d,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
    WHERE holder_id <= 19
      AND product IN ('HOME', 'TRAVEL')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(premium_filled, 2) OVER (PARTITION BY holder_id, status ORDER BY premium ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_filled,
        SUM(premium) OVER (PARTITION BY holder_id, status ORDER BY premium ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        MIN(premium_filled) OVER (PARTITION BY product) AS product_min_premium_filled,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id, status ORDER BY premium ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium
    FROM prepared
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        premium_filled,
        start_date_plus_30d,
        second_premium_filled,
        running_premium,
        product_min_premium_filled,
        last_known_premium,
        LEAD(start_date, 2) OVER (PARTITION BY status, product ORDER BY second_premium_filled, id) AS next2_start_date,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS rows_in_end_date_month,
        ROUND(CUME_DIST() OVER (PARTITION BY status, product ORDER BY second_premium_filled), 3) AS second_premium_filled_cume_dist,
        FIRST_VALUE(last_known_premium) IGNORE NULLS OVER (PARTITION BY status, product ORDER BY second_premium_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_premium
    FROM sequenced
)
-- Subtotals with CUBE
SELECT
    holder_id,
    product,
    status,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM with_trends
GROUP BY CUBE(holder_id, product, status)
ORDER BY holder_id NULLS LAST, product NULLS LAST, status NULLS LAST

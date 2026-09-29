WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        QUARTER(end_date) AS end_date_quarter,
        ADD_MONTHS(end_date, 6) AS end_date_plus_6m
    FROM policies
    WHERE start_date BETWEEN DATE '2024-07-19' AND DATE '2025-05-30'
      AND product IN ('HOME', 'HEALTH', 'LIFE')
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ADD_MONTHS(start_date, 3) AS start_date_plus_3m,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM cleaned
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY end_date_quarter)) AS distinct_holder_id_in_end_date_quarter,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY end_date_quarter)) AS distinct_product_in_end_date_quarter,
        NTILE(10) OVER (ORDER BY end_date, id) AS end_date_decile,
        LEAD(premium_adj, 3) OVER w_all_by_start_date AS next3_premium_adj,
        MIN(premium) OVER (PARTITION BY end_date_quarter) AS end_date_quarter_min_premium,
        MAX(premium_adj) OVER (PARTITION BY end_date_quarter) AS end_date_quarter_max_premium_adj
    FROM annotated
    WINDOW w_all_by_start_date AS (ORDER BY start_date, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_product_in_end_date_quarter, product ORDER BY premium, id), 3) AS premium_cume_dist,
        NTH_VALUE(end_date_quarter_min_premium, 2) OVER (ORDER BY distinct_product_in_end_date_quarter NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_quarter_min_premium,
        COUNT(*) OVER (PARTITION BY end_date_quarter) AS rows_in_end_date_quarter,
        NTILE(10) OVER (PARTITION BY distinct_product_in_end_date_quarter, product ORDER BY premium, id) AS premium_decile
    FROM metrics
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date_quarter, status, end_date_quarter_min_premium
    FROM with_windows
) src
PIVOT (
    MIN(end_date_quarter_min_premium)
    FOR status IN ('CANCELLED', 'LAPSED', 'EXPIRED', 'ACTIVE')
)

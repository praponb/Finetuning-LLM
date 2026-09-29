WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        DATE_ADD(end_date, 365) AS end_date_plus_365d
    FROM policies
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        premium_filled,
        end_date_plus_365d,
        SUM(premium) OVER w_product AS product_sum_premium,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROUND(premium_filled / NULLIF(SUM(premium_filled) OVER (PARTITION BY product), 0), 4) AS premium_filled_share_of_product,
        SUM(premium_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_filled_sum_7
    FROM base_policys
    WINDOW w_product AS (PARTITION BY product)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY status, holder_id ORDER BY end_date_plus_365d), 3) AS end_date_plus_365d_cume_dist,
        LEAD(premium_filled, 1, 0) OVER (PARTITION BY holder_id ORDER BY premium_filled ASC, id) AS next_premium_filled,
        LAST_VALUE(end_date_plus_365d) OVER (PARTITION BY status, holder_id ORDER BY end_date_plus_365d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date_plus_365d,
        COUNT(*) OVER w_start_date_year_status AS rows_in_start_date_year_status
    FROM with_windows
    WINDOW w_start_date_year_status AS (PARTITION BY YEAR(start_date), status)
)
SELECT
    holder_id,
    COALESCE(status, 'ALL') AS status,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN status IN ('CANCELLED', 'LAPSED', 'EXPIRED') THEN 1 END) / COUNT(*), 1) AS pct_status_in_cancelled_lapsed_e
FROM ranked
GROUP BY GROUPING SETS ((holder_id, status), (holder_id), ())

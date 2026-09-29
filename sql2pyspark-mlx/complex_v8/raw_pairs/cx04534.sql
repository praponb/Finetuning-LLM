WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE premium >= 1541.8
      AND start_date >= DATE '2024-08-27'
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        premium,
        status,
        holder_name,
        holder_joined_on,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, MONTH(holder_joined_on)), 2) AS premium_vs_status_holder_joined_on_month_avg,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(holder_joined_on), holder_id ORDER BY premium, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        MIN(premium) OVER (PARTITION BY status, MONTH(holder_joined_on)) AS status_holder_joined_on_month_min_premium,
        LEAD(holder_joined_on) OVER w_all_by_premium AS next_holder_joined_on,
        LEAD(holder_joined_on, 3) OVER w_all_by_premium AS next3_holder_joined_on,
        ROUND(AVG(premium) OVER (PARTITION BY status, MONTH(holder_joined_on)), 2) AS status_holder_joined_on_month_avg_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(holder_joined_on), holder_id ORDER BY premium), 3) AS premium_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY MONTH(holder_joined_on), holder_id ORDER BY premium, id) AS seq_no,
        NTILE(3) OVER (ORDER BY premium ASC, id) AS premium_tercile,
        COUNT(*) OVER w_status_holder_joined_on_mont AS rows_in_status_holder_joined_on_month
    FROM combined_details_holder
    WINDOW w_all_by_premium AS (ORDER BY premium ASC, id), w_status_holder_joined_on_mont AS (PARTITION BY status, MONTH(holder_joined_on))
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        status,
        holder_joined_on,
        premium_vs_status_holder_joined_on_month_avg,
        premium_avg_1_prior,
        status_holder_joined_on_month_min_premium,
        next3_holder_joined_on,
        status_holder_joined_on_month_avg_premium,
        premium_tercile,
        rows_in_status_holder_joined_on_month,
        MAX(premium_cume_dist) OVER (PARTITION BY premium_tercile ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_cume_dist_so_far,
        NTILE(4) OVER w_premium_tercile_by_premium AS premium_quartile,
        NTH_VALUE(premium_cume_dist, 2) OVER (PARTITION BY premium_tercile ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_cume_dist
    FROM ranked
    WINDOW w_premium_tercile_by_premium AS (PARTITION BY premium_tercile ORDER BY premium, id)
)
SELECT
    id,
    holder_id,
    premium,
    premium_avg_1_prior,
    status_holder_joined_on_month_min_premium,
    status_holder_joined_on_month_avg_premium,
    premium_tercile,
    rows_in_status_holder_joined_on_month,
    max_premium_cume_dist_so_far,
    premium_quartile,
    second_premium_cume_dist,
    GREATEST(second_premium_cume_dist, premium_avg_1_prior) AS greatest_second_premium_cume_dist_premium_avg_1_,
    SUBSTRING(TRIM(status), 1, 4) AS status_prefix
FROM ranked_2
ORDER BY status_holder_joined_on_month_avg_premium DESC, premium DESC

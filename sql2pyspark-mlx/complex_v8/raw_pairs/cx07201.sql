WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_ADD(end_date, 7) AS end_date_plus_7d
    FROM policies
    WHERE end_date IS NOT NULL
      AND start_date >= DATE '2024-12-02'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        end_date_plus_7d,
        MAX(premium) OVER w_holder_id_end_date_month AS holder_id_end_date_month_max_premium,
        NTH_VALUE(end_date_plus_7d, 2) OVER (PARTITION BY status, holder_id ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_plus_7d,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, MONTH(end_date)), 2) AS premium_vs_holder_id_end_date_month_avg,
        RANK() OVER (PARTITION BY status, holder_id ORDER BY premium NULLS LAST) AS premium_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, MONTH(end_date)), 0), 4) AS premium_share_of_holder_id_end_date_month,
        LEAD(end_date_plus_7d) OVER w_all_by_end_date_plus_7d AS next_end_date_plus_7d,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id, MONTH(end_date))) AS distinct_product_in_holder_id_end_date_month,
        LAST_VALUE(end_date) OVER (PARTITION BY status, holder_id ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        LAST_VALUE(start_date) OVER (PARTITION BY MONTH(end_date_plus_7d), product ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROUND(CUME_DIST() OVER w_status_holder_id_by_premium_3, 3) AS premium_cume_dist
    FROM prepared_policys
    WINDOW w_holder_id_end_date_month AS (PARTITION BY holder_id, MONTH(end_date)), w_all_by_end_date_plus_7d AS (ORDER BY end_date_plus_7d DESC, id), w_status_holder_id_by_premium_3 AS (PARTITION BY status, holder_id ORDER BY premium NULLS LAST)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        DATE_TRUNC('MONTH', last_end_date) AS last_end_date_month,
        SUM(CASE WHEN status IN ('CANCELLED', 'LAPSED', 'ACTIVE') THEN premium_cume_dist ELSE 0 END) AS premium_cume_dist_status_in_cancelled_lapsed_a,
        MAX(holder_id_end_date_month_max_premium) AS max_holder_id_end_date_month_max_premium,
        SUM(CASE WHEN product = 'TRAVEL' THEN 1 ELSE 0 END) AS n_product_travel
    FROM ranked
    GROUP BY DATE_TRUNC('MONTH', end_date), DATE_TRUNC('MONTH', last_end_date)
    HAVING SUM(distinct_product_in_holder_id_end_date_month) > 3
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(n_product_travel / NULLIF(SUM(n_product_travel) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month)), 0), 4) AS n_product_travel_share_of_end_date_month_month,
        ROUND(AVG(n_product_travel) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_month)), 2) AS end_date_month_month_avg_n_product_travel,
        NTILE(3) OVER w_n_product_travel_end_date_mo AS n_product_travel_tercile
    FROM totals
    WINDOW w_n_product_travel_end_date_mo AS (PARTITION BY n_product_travel, YEAR(end_date_month) ORDER BY n_product_travel, end_date_month, last_end_date_month)
)
SELECT
    end_date_month,
    last_end_date_month,
    premium_cume_dist_status_in_cancelled_lapsed_a,
    max_holder_id_end_date_month_max_premium,
    n_product_travel,
    n_product_travel_share_of_end_date_month_month,
    n_product_travel_tercile,
    DATE_FORMAT(last_end_date_month, 'MMM yyyy') AS last_end_date_month_label
FROM ranked_2
WHERE premium_cume_dist_status_in_cancelled_lapsed_a BETWEEN 0.8 AND 4.5
ORDER BY last_end_date_month_label DESC NULLS FIRST, last_end_date_month DESC NULLS FIRST

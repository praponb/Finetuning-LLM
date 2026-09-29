WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE end_date >= DATE '2026-02-26'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.product_or_unknown,
        p.holder_name
    FROM valid_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        product_or_unknown,
        holder_name,
        MIN(premium) OVER (PARTITION BY product_or_unknown, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        premium - LAG(premium) OVER (PARTITION BY holder_id ORDER BY end_date, id) AS premium_change,
        ROW_NUMBER() OVER (PARTITION BY product_or_unknown, holder_id ORDER BY premium, id) AS rn,
        ROUND(AVG(premium) OVER w_status_product_or_unknown, 2) AS status_product_or_unknown_avg_premium,
        NTH_VALUE(premium, 2) OVER (ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date)), 2) AS holder_id_start_date_month_avg_premium,
        NTILE(3) OVER (ORDER BY start_date DESC NULLS FIRST, id) AS start_date_tercile,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        ROW_NUMBER() OVER w_all_by_start_date AS start_date_seq,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY product_or_unknown, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        MAX(premium) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date)) AS holder_id_start_date_month_max_premium,
        ROUND(AVG(premium) OVER (PARTITION BY product_or_unknown, holder_id ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        SIZE(COLLECT_SET(product_or_unknown) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date))) AS distinct_product_or_unknown_in_holder_id_start_d,
        LEAD(start_date, 3) OVER (PARTITION BY product_or_unknown, holder_id ORDER BY premium, id) AS next3_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date)), 0), 4) AS premium_share_of_holder_id_start_date_month
    FROM joined_holder
    WINDOW w_status_product_or_unknown AS (PARTITION BY status, product_or_unknown), w_all_by_start_date AS (ORDER BY start_date DESC NULLS FIRST, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        product_or_unknown,
        status_product_or_unknown_avg_premium,
        start_date_tercile,
        premium_avg_3,
        holder_id_start_date_month_max_premium,
        distinct_product_or_unknown_in_holder_id_start_d,
        next3_start_date,
        premium_share_of_holder_id_start_date_month,
        ROUND(min_premium_so_far / NULLIF(SUM(min_premium_so_far) OVER w_start_date_seq_product_or_un, 0), 4) AS min_premium_so_far_share_of_start_date_seq_produ,
        COUNT(*) OVER (PARTITION BY YEAR(next3_start_date) ORDER BY start_date_seq DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(second_premium) IGNORE NULLS OVER (PARTITION BY YEAR(start_date), product_or_unknown ORDER BY premium_change ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_premium,
        SUM(rn) OVER (PARTITION BY rn ORDER BY start_date_tercile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rn,
        NTILE(10) OVER w_next3_start_date_year_by_sta_2 AS start_date_seq_decile
    FROM metrics
    WINDOW w_start_date_seq_product_or_un AS (PARTITION BY start_date_seq, product_or_unknown), w_next3_start_date_year_by_sta_2 AS (PARTITION BY YEAR(next3_start_date) ORDER BY start_date_seq DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    start_date,
    product_or_unknown,
    status_product_or_unknown_avg_premium,
    start_date_tercile,
    next3_start_date,
    premium_share_of_holder_id_start_date_month,
    running_count,
    running_rn,
    ROUND(premium_avg_3 * 1.5, 2) AS premium_avg_3_adj,
    CASE WHEN premium_avg_3 >= 3320.0 THEN 'HIGH' WHEN premium_avg_3 >= 1880.0 THEN 'MEDIUM' ELSE 'LOW' END AS premium_avg_3_tier
FROM with_windows
ORDER BY premium_avg_3_tier

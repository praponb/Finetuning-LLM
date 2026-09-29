WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
    WHERE status IN ('ACTIVE', 'CANCELLED', 'EXPIRED')
      AND start_date >= DATE '2024-11-20'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.product_ref,
        p.holder_name,
        p.segment AS holder_segment
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_holder_segment_holder_id AS rows_in_holder_segment_holder_id,
        SUM(premium) OVER (PARTITION BY holder_segment, YEAR(end_date)) AS holder_segment_end_date_year_sum_premium,
        LAG(premium, 2) OVER w_all_by_premium AS prev2_premium,
        MAX(premium) OVER (ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        NTILE(5) OVER w_all_by_premium AS premium_quintile,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY premium, id)) AS days_since_prev_end_date,
        premium - LAG(premium) OVER w_holder_id_holder_segment_by AS premium_change,
        RANK() OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id) AS premium_rank,
        MAX(premium) OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        ROW_NUMBER() OVER w_all_by_premium AS premium_seq
    FROM joined_holder
    WINDOW w_holder_segment_holder_id AS (PARTITION BY holder_segment, holder_id), w_all_by_premium AS (ORDER BY premium NULLS LAST, id), w_holder_id_holder_segment_by AS (PARTITION BY holder_id, holder_segment ORDER BY premium, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown,
        prev2_premium - premium_quintile AS prev2_premium_minus_premium_quintile
    FROM with_windows
),
stats AS (
    -- Summarise per group
    SELECT
        holder_id,
        holder_segment,
        DATE_TRUNC('MONTH', end_date_month_end) AS end_date_month_end_month,
        ARRAY_SORT(COLLECT_SET(holder_segment)) AS holder_segment_list,
        ROUND(STDDEV(max_premium_so_far), 2) AS stddev_max_premium_so_far,
        DATEDIFF(MAX(end_date_month_end), MIN(end_date_month_end)) AS end_date_month_end_span_days,
        COUNT(*) AS row_count,
        SUM(CASE WHEN UPPER(TRIM(holder_name)) LIKE 'M%' THEN 1 ELSE 0 END) AS n_upper_trim_holder_name_like
    FROM labeled
    GROUP BY holder_id, holder_segment, DATE_TRUNC('MONTH', end_date_month_end)
)
-- Final projection
SELECT
    holder_id,
    holder_segment_list,
    stddev_max_premium_so_far,
    end_date_month_end_span_days,
    row_count,
    n_upper_trim_holder_name_like
FROM stats

WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        CASE WHEN product IN ('AUTO', 'HOME') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE status <> 'ACTIVE'
      AND premium >= 2096.6
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.status_ref,
        s.product_group,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on,
        p.holder_name
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(product_group) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), status)) AS distinct_product_group_in_holder_joined_on_month,
        SUM(premium) OVER (PARTITION BY YEAR(end_date), holder_segment) AS end_date_year_holder_segment_sum_premium,
        MIN(premium) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium DESC, id), 3) AS premium_percent_rank,
        ROUND(AVG(premium) OVER (PARTITION BY product_group ORDER BY holder_joined_on DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        LEAD(premium) OVER (PARTITION BY product_group, holder_id ORDER BY end_date DESC, id) AS next_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, product), 2) AS premium_vs_status_product_avg,
        ROUND(CUME_DIST() OVER (ORDER BY start_date, id), 3) AS start_date_cume_dist,
        ROW_NUMBER() OVER (ORDER BY premium DESC, id) AS seq_no,
        LAST_VALUE(start_date) OVER (ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY premium DESC, id)) AS days_since_prev_start_date,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on), status) AS holder_joined_on_month_status_sum_premium
    FROM joined_holder
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        product_group,
        holder_segment,
        holder_joined_on,
        holder_name,
        distinct_product_group_in_holder_joined_on_month,
        end_date_year_holder_segment_sum_premium,
        min_premium_so_far,
        premium_vs_status_product_avg,
        start_date_cume_dist,
        seq_no,
        last_start_date,
        days_since_prev_start_date,
        LAST_VALUE(seq_no) OVER (ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seq_no,
        NTH_VALUE(premium, 2) OVER (PARTITION BY holder_id, YEAR(last_start_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        RANK() OVER (PARTITION BY holder_id, YEAR(last_start_date) ORDER BY start_date) AS start_date_rank,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_segment ORDER BY premium_vs_status_product_avg ASC), 3) AS premium_vs_status_product_avg_percent_rank,
        MAX(premium_avg_7) OVER (ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_avg_7_so_far,
        ROUND(premium_percent_rank - AVG(premium_percent_rank) OVER (PARTITION BY holder_id), 2) AS premium_percent_rank_vs_holder_id_avg
    FROM ranked
)
SELECT
    id,
    premium,
    start_date,
    end_date,
    status,
    holder_segment,
    holder_name,
    distinct_product_group_in_holder_joined_on_month,
    end_date_year_holder_segment_sum_premium,
    premium_vs_status_product_avg,
    last_start_date,
    last_seq_no,
    second_premium,
    premium_vs_status_product_avg_percent_rank,
    premium_percent_rank_vs_holder_id_avg
FROM sequenced

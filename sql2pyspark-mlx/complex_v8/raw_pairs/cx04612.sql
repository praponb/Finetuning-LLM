WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        DATE_ADD(start_date, 7) AS start_date_plus_7d,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE holder_id <= 12
),
with_parent_holder AS (
    -- Join policyholders
    SELECT s.*, p.risk_score AS holder_risk_score, p.segment AS holder_segment
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        premium_bucket,
        holder_risk_score,
        holder_segment,
        SIZE(COLLECT_SET(holder_segment) OVER w_end_date_month_holder_id) AS distinct_holder_segment_in_end_date_month_holder,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY product, holder_segment ORDER BY premium_bucket DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_3,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY product, holder_segment ORDER BY premium_bucket DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        SUM(holder_risk_score) OVER (PARTITION BY holder_segment, YEAR(start_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST), 3) AS end_date_cume_dist,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_end_date_ties, 3) AS end_date_percent_rank,
        ROUND(CUME_DIST() OVER w_start_date_plus_7d_year_by_s, 3) AS start_date_cume_dist,
        NTILE(4) OVER (PARTITION BY product, holder_segment ORDER BY premium_bucket DESC NULLS FIRST, id) AS premium_bucket_quartile,
        SIZE(COLLECT_SET(holder_segment) OVER (PARTITION BY product, MONTH(start_date_plus_7d))) AS distinct_holder_segment_in_product_start_date_pl,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_7,
        LEAD(premium_bucket) OVER (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST, id) AS next_premium_bucket
    FROM with_parent_holder
    WINDOW w_end_date_month_holder_id AS (PARTITION BY MONTH(end_date), holder_id), w_holder_id_by_end_date_ties AS (PARTITION BY holder_id ORDER BY end_date DESC NULLS FIRST), w_start_date_plus_7d_year_by_s AS (PARTITION BY YEAR(start_date_plus_7d) ORDER BY start_date, id)
),
summary AS (
    -- Summarise per group
    SELECT
        holder_segment,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        SUM(CASE WHEN product = 'HEALTH' THEN 1 ELSE 0 END) AS n_product_health,
        MAX(running_holder_risk_score) AS max_running_holder_risk_score,
        MIN(end_date) AS first_end_date,
        ROUND(AVG(holder_risk_score_avg_7), 2) AS avg_holder_risk_score_avg_7,
        MIN(start_date) AS first_start_date,
        ROUND(SUM(next_premium_bucket) / NULLIF(SUM(distinct_holder_segment_in_end_date_month_holder), 0), 3) AS next_premium_bucket_to_distinct_holder_segment_i
    FROM ranked
    WHERE start_date IS NOT NULL
    GROUP BY holder_segment, DATE_FORMAT(start_date, 'yyyy-MM')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        holder_segment,
        start_date_ym,
        n_product_health,
        max_running_holder_risk_score,
        avg_holder_risk_score_avg_7,
        first_start_date,
        MAX(avg_holder_risk_score_avg_7) OVER w_first_end_date_month AS first_end_date_month_max_avg_holder_risk_score_a,
        LAG(n_product_health) OVER (ORDER BY max_running_holder_risk_score NULLS LAST, holder_segment, start_date_ym) AS prev_n_product_health,
        SIZE(COLLECT_SET(holder_segment) OVER (PARTITION BY DATE_TRUNC('MONTH', first_end_date))) AS distinct_holder_segment_in_first_end_date_month
    FROM summary
    WINDOW w_first_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', first_end_date))
)
SELECT
    holder_segment,
    start_date_ym,
    n_product_health,
    max_running_holder_risk_score,
    avg_holder_risk_score_avg_7,
    first_start_date,
    first_end_date_month_max_avg_holder_risk_score_a,
    prev_n_product_health,
    distinct_holder_segment_in_first_end_date_month,
    CASE WHEN n_product_health >= 7 THEN 'A' WHEN n_product_health >= 2 THEN 'B' ELSE 'C' END AS n_product_health_tier,
    FLOOR(n_product_health / 2) * 2 AS n_product_health_bucket
FROM sequenced

WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE (premium >= 1042.8 OR product NOT IN ('HEALTH', 'AUTO'))
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.days_since_end_date,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM filtered_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        product,
        DATE_TRUNC('MONTH', holder_joined_on) AS holder_joined_on_month,
        MIN(holder_risk_score) AS min_holder_risk_score,
        ROUND(AVG(holder_risk_score), 2) AS avg_holder_risk_score,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        SUM(holder_risk_score) AS total_holder_risk_score,
        PERCENTILE_APPROX(holder_risk_score, 0.5) AS median_holder_risk_score
    FROM enriched
    WHERE holder_risk_score > 0.2
    GROUP BY DATE_TRUNC('MONTH', end_date), product, DATE_TRUNC('MONTH', holder_joined_on)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(distinct_holder_ids) OVER (PARTITION BY distinct_holder_ids ORDER BY avg_holder_risk_score NULLS LAST, end_date_month, product, holder_joined_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_holder_ids,
        ROUND(AVG(min_holder_risk_score) OVER (PARTITION BY YEAR(end_date_month) ORDER BY median_holder_risk_score NULLS LAST, end_date_month, product, holder_joined_on_month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS min_holder_risk_score_avg_2,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_holder_ids ORDER BY avg_holder_risk_score NULLS LAST), 3) AS avg_holder_risk_score_cume_dist
    FROM stats
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(first_distinct_holder_ids, 2) OVER (ORDER BY min_holder_risk_score ASC, end_date_month, product, holder_joined_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_distinct_holder_ids,
        RANK() OVER (ORDER BY min_holder_risk_score ASC, end_date_month, product, holder_joined_on_month) AS min_holder_risk_score_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY min_holder_risk_score ASC), 3) AS min_holder_risk_score_percent_rank
    FROM metrics
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        end_date_month,
        product,
        holder_joined_on_month,
        min_holder_risk_score,
        avg_holder_risk_score,
        distinct_holder_ids,
        median_holder_risk_score,
        first_distinct_holder_ids,
        avg_holder_risk_score_cume_dist,
        second_first_distinct_holder_ids,
        min_holder_risk_score_rank,
        min_holder_risk_score_percent_rank,
        NTH_VALUE(min_holder_risk_score_avg_2, 2) OVER (PARTITION BY product, distinct_holder_ids ORDER BY min_holder_risk_score_avg_2 DESC NULLS FIRST, end_date_month, product, holder_joined_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_holder_risk_score_avg_2,
        RANK() OVER (PARTITION BY product, distinct_holder_ids ORDER BY min_holder_risk_score_avg_2 DESC NULLS FIRST) AS min_holder_risk_score_avg_2_rank,
        NTILE(3) OVER w_product_distinct_holder_ids_3 AS min_holder_risk_score_avg_2_tercile,
        MAX(min_holder_risk_score_avg_2) OVER (PARTITION BY first_distinct_holder_ids) AS first_distinct_holder_ids_max_min_holder_risk_sc
    FROM metrics_2
    WINDOW w_product_distinct_holder_ids_3 AS (PARTITION BY product, distinct_holder_ids ORDER BY min_holder_risk_score_avg_2 DESC NULLS FIRST, end_date_month, product, holder_joined_on_month)
)
SELECT
    min_holder_risk_score_avg_2_rank,
    distinct_holder_ids,
    min_holder_risk_score_rank,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(first_distinct_holder_ids_max_min_holder_risk_sc), 2) AS stddev_first_distinct_holder_ids_max_min_holder_,
    SUM(median_holder_risk_score) FILTER (WHERE product <> 'AUTO') AS median_holder_risk_score_product_auto,
    ROUND(SUM(first_distinct_holder_ids) / NULLIF(SUM(min_holder_risk_score), 0), 3) AS first_distinct_holder_ids_to_min_holder_risk_sco
FROM with_trends
GROUP BY ROLLUP(min_holder_risk_score_avg_2_rank, distinct_holder_ids, min_holder_risk_score_rank)
ORDER BY min_holder_risk_score_avg_2_rank NULLS LAST, distinct_holder_ids NULLS LAST, min_holder_risk_score_rank NULLS LAST

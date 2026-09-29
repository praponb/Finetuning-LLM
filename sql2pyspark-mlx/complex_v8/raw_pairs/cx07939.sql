WITH base AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE end_date BETWEEN DATE '2025-12-05' AND DATE '2026-09-23'
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM base s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        RANK() OVER w_all_by_holder_risk_score AS holder_risk_score_rank,
        RANK() OVER (ORDER BY holder_risk_score DESC) AS holder_risk_score_rank_2,
        LEAD(end_date) OVER (PARTITION BY holder_region ORDER BY holder_risk_score DESC, id) AS next_end_date,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date))) AS distinct_product_in_end_date_month,
        LAST_VALUE(holder_risk_score) IGNORE NULLS OVER (ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_holder_risk_score,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_region, holder_id)) AS distinct_product_in_holder_region_holder_id,
        MAX(holder_risk_score) OVER w_holder_region_holder_id AS holder_region_holder_id_max_holder_risk_score,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER w_end_date_month, 2) AS holder_risk_score_vs_end_date_month_avg,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_region, holder_id), 0), 4) AS holder_risk_score_share_of_holder_region_holder_,
        FIRST_VALUE(holder_risk_score) IGNORE NULLS OVER (ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_holder_risk_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY holder_risk_score DESC), 3) AS holder_risk_score_percent_rank
    FROM enriched_holder
    WINDOW w_all_by_holder_risk_score AS (ORDER BY holder_risk_score DESC, id), w_holder_region_holder_id AS (PARTITION BY holder_region, holder_id), w_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date))
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        product,
        end_date,
        last_known_end_date,
        holder_risk_score_rank_2,
        next_end_date,
        distinct_product_in_end_date_month,
        last_known_holder_risk_score,
        distinct_product_in_holder_region_holder_id,
        holder_risk_score_vs_end_date_month_avg,
        holder_risk_score_share_of_holder_region_holder_,
        first_known_holder_risk_score,
        COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown,
        FLOOR(holder_risk_score_rank_2 / 2) * 2 AS holder_risk_score_rank_2_bucket,
        LENGTH(TRIM(product)) AS product_len
    FROM with_trends
    WHERE last_known_end_date >= DATE '2026-01-24'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(last_known_holder_risk_score - AVG(last_known_holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_end_date), product), 2) AS last_known_holder_risk_score_vs_last_known_end_d,
        LEAD(next_end_date) OVER w_product_holder_risk_score_ra AS next_next_end_date,
        MIN(holder_risk_score_rank_2) OVER (PARTITION BY product) AS product_min_holder_risk_score_rank_2,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product_len, holder_region_or_unknown ORDER BY holder_risk_score_rank_2 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_2,
        NTH_VALUE(last_known_end_date, 2) OVER (PARTITION BY product_len, holder_region_or_unknown ORDER BY holder_risk_score_rank_2 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_end_date,
        ROUND(holder_risk_score_rank_2 - AVG(holder_risk_score_rank_2) OVER w_product, 2) AS holder_risk_score_rank_2_vs_product_avg,
        LAST_VALUE(last_known_holder_risk_score) OVER (PARTITION BY product, holder_risk_score_rank_2 ORDER BY last_known_holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_known_holder_risk_score
    FROM enriched
    WINDOW w_product_holder_risk_score_ra AS (PARTITION BY product, holder_risk_score_rank_2 ORDER BY last_known_holder_risk_score, id), w_product AS (PARTITION BY product)
)
SELECT
    id,
    product,
    end_date,
    last_known_end_date,
    holder_risk_score_rank_2,
    distinct_product_in_end_date_month,
    last_known_holder_risk_score,
    distinct_product_in_holder_region_holder_id,
    holder_risk_score_vs_end_date_month_avg,
    holder_risk_score_share_of_holder_region_holder_,
    first_known_holder_risk_score,
    holder_region_or_unknown,
    holder_risk_score_rank_2_bucket,
    product_len,
    next_next_end_date,
    product_min_holder_risk_score_rank_2,
    holder_risk_score_rank_2_vs_product_avg,
    last_last_known_holder_risk_score
FROM ranked
ORDER BY last_last_known_holder_risk_score DESC, id
LIMIT 5

WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status IS NOT NULL
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.region AS holder_region,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY product, status ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        SIZE(COLLECT_SET(holder_id) OVER w_status_product) AS distinct_holder_id_in_status_product,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY holder_id ORDER BY premium ASC, id)) AS days_since_prev_end_date,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY holder_region, holder_id ORDER BY holder_risk_score DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_7,
        DENSE_RANK() OVER (PARTITION BY holder_id ORDER BY premium ASC) AS premium_dense_rank,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY product, status ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        MIN(premium) OVER (PARTITION BY product, status ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        LEAD(end_date, 2) OVER (PARTITION BY holder_id ORDER BY premium ASC, id) AS next2_end_date,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_premium_ties, 3) AS premium_percent_rank,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        MAX(holder_risk_score) OVER (PARTITION BY product, status ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_6_prior,
        SUM(holder_risk_score) OVER (PARTITION BY status, product) AS status_product_sum_holder_risk_score
    FROM enriched
    WINDOW w_status_product AS (PARTITION BY status, product), w_holder_id_by_premium_ties AS (PARTITION BY holder_id ORDER BY premium ASC)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        status,
        holder_name,
        holder_risk_score,
        distinct_holder_id_in_status_product,
        days_since_prev_end_date,
        second_end_date,
        min_premium_so_far,
        premium_percent_rank,
        last_known_premium,
        status_product_sum_holder_risk_score,
        FIRST_VALUE(premium_percent_rank) IGNORE NULLS OVER (PARTITION BY distinct_holder_id_in_status_product, MONTH(first_known_end_date) ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_percent_rank,
        MAX(premium_dense_rank) OVER (ORDER BY end_date ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_dense_rank_max_6_prior,
        ROUND(premium_dense_rank / NULLIF(SUM(premium_dense_rank) OVER w_holder_region, 0), 4) AS premium_dense_rank_share_of_holder_region
    FROM windowed
    WINDOW w_holder_region AS (PARTITION BY holder_region)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(second_end_date) AS second_end_date_year,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(*) AS n_rows,
        ROUND(SUM(min_premium_so_far) / NULLIF(SUM(premium_percent_rank), 0), 3) AS min_premium_so_far_to_premium_percent_rank_ratio,
        SUM(CASE WHEN status = 'ACTIVE' THEN holder_risk_score ELSE 0 END) AS holder_risk_score_status_active
    FROM with_trends
    WHERE premium < 3719.7
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        second_end_date_year,
        distinct_holder_ids,
        min_premium_so_far_to_premium_percent_rank_ratio,
        holder_risk_score_status_active,
        ROUND(second_end_date_year - AVG(second_end_date_year) OVER (PARTITION BY n_rows), 2) AS second_end_date_year_vs_n_rows_avg,
        ROUND(distinct_holder_ids - AVG(distinct_holder_ids) OVER (PARTITION BY n_rows), 2) AS distinct_holder_ids_vs_n_rows_avg,
        LAG(n_rows) OVER (ORDER BY n_rows, second_end_date_year) AS prev_n_rows,
        NTILE(10) OVER (ORDER BY n_rows, second_end_date_year) AS n_rows_decile
    FROM stats
)
SELECT
    second_end_date_year,
    distinct_holder_ids,
    holder_risk_score_status_active,
    second_end_date_year_vs_n_rows_avg,
    distinct_holder_ids_vs_n_rows_avg,
    prev_n_rows,
    COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled,
    ROUND(distinct_holder_ids / NULLIF(n_rows_decile, 0), 3) AS distinct_holder_ids_per_n_rows_decile
FROM windowed_2
ORDER BY second_end_date_year NULLS LAST, distinct_holder_ids_vs_n_rows_avg DESC
LIMIT 20

WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        DATE_TRUNC('YEAR', start_date) AS start_date_year
    FROM policies
    WHERE start_date IS NOT NULL
      AND product <> 'LIFE'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(start_date_year, 2) OVER (PARTITION BY MONTH(start_date_year) ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_year,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date_ties, 3) AS end_date_percent_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS premium_vs_product_holder_id_avg,
        COUNT(*) OVER (PARTITION BY product, holder_id) AS rows_in_product_holder_id,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id, status)) AS distinct_product_in_holder_id_status,
        SUM(premium) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        MIN(premium) OVER w_product_holder_id AS product_holder_id_min_premium,
        SUM(premium) OVER (ORDER BY premium, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_sum_4,
        ROUND(AVG(premium) OVER (PARTITION BY product, holder_id), 2) AS product_holder_id_avg_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product, holder_id)) AS distinct_status_in_product_holder_id
    FROM prepared_policys
    WINDOW w_all_by_end_date_ties AS (ORDER BY end_date), w_product_holder_id AS (PARTITION BY product, holder_id)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    start_date_year,
    second_start_date_year,
    end_date_percent_rank,
    rows_in_product_holder_id,
    distinct_product_in_holder_id_status,
    product_holder_id_min_premium,
    product_holder_id_avg_premium,
    TO_DATE(start_date_year) AS start_date_year_date
FROM with_windows
WHERE MONTH(start_date_year) IN (2, 3)
ORDER BY status DESC, end_date_percent_rank DESC

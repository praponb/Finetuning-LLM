WITH valid AS (
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
    WHERE start_date < DATE '2025-07-07'
      AND YEAR(end_date) = 2026
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id IS NOT NULL
              AND c.approved_amount < 14595.6
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        ROUND(SUM(premium), 2) AS total_premium,
        DATEDIFF(MAX(end_date), MIN(end_date)) AS end_date_span_days,
        COUNT(*) FILTER (WHERE product = 'LIFE') AS n_product_life,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS n_rows,
        SUM(premium) AS total_premium_2
    FROM filtered
    GROUP BY product, DATE_TRUNC('MONTH', start_date), DATE_FORMAT(end_date, 'yyyy-MM')
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        product,
        start_date_month,
        end_date_ym,
        total_premium,
        end_date_span_days,
        n_product_life,
        LAST_VALUE(distinct_ids) IGNORE NULLS OVER (PARTITION BY n_rows, distinct_ids ORDER BY end_date_span_days, product, start_date_month, end_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_ids,
        NTILE(4) OVER w_n_rows_distinct_ids_by_end_d_2 AS end_date_span_days_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_rows, distinct_ids ORDER BY end_date_span_days, product, start_date_month, end_date_ym), 3) AS end_date_span_days_percent_rank,
        SUM(distinct_ids) OVER (PARTITION BY n_rows, distinct_ids ORDER BY end_date_span_days, product, start_date_month, end_date_ym ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS distinct_ids_sum_2_prior,
        NTH_VALUE(total_premium, 2) OVER (PARTITION BY n_rows, distinct_ids ORDER BY end_date_span_days, product, start_date_month, end_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_premium,
        LAST_VALUE(start_date_month) OVER (PARTITION BY n_rows, distinct_ids ORDER BY end_date_span_days, product, start_date_month, end_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date_month
    FROM totals
    WINDOW w_n_rows_distinct_ids_by_end_d_2 AS (PARTITION BY n_rows, distinct_ids ORDER BY end_date_span_days, product, start_date_month, end_date_ym)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE distinct_ids_sum_2_prior < (SELECT MAX(distinct_ids_sum_2_prior) FROM sequenced)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_start_date_month) - UNIX_TIMESTAMP(start_date_month)) / 3600, 1) AS hours_start_date_month_to_last_start_date_month
    FROM without_matches
),
without_matches_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE end_date_span_days > (SELECT AVG(end_date_span_days) FROM enriched)
)
SELECT product, start_date_month, measure, value
FROM (
    SELECT product, start_date_month, second_total_premium, hours_start_date_month_to_last_start_date_month
    FROM without_matches_2
) src
UNPIVOT (
    value FOR measure IN (second_total_premium, hours_start_date_month_to_last_start_date_month)
) unp

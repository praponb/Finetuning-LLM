WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', status, product) AS status_product_label,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE (status = 'LAPSED' OR start_date >= DATE '2024-08-27')
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_policys s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_type IN ('theft', 'collision')
              AND c.claim_date IS NOT NULL
        )
      AND s.end_date < DATE '2026-11-25'
),
summary AS (
    -- Summarise per group
    SELECT
        status,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        MAX(end_date) AS last_end_date,
        MIN(end_date) AS first_end_date,
        COUNT(*) AS row_count
    FROM qualified
    GROUP BY status
    HAVING AVG(days_start_date_to_end_date) > -560
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', first_end_date), row_count ORDER BY last_end_date ASC), 3) AS last_end_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_end_date), row_count ORDER BY last_end_date ASC), 3) AS last_end_date_percent_rank,
        MAX(row_count) OVER (PARTITION BY DATE_TRUNC('MONTH', first_end_date)) AS first_end_date_month_max_row_count,
        ROUND(AVG(distinct_holder_ids) OVER w_first_end_date_month, 2) AS first_end_date_month_avg_distinct_holder_ids
    FROM summary
    WINDOW w_first_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', first_end_date))
)
-- Native SQL UNPIVOT (wide to long)
SELECT status, field, value
FROM (
    SELECT status, first_end_date_month_avg_distinct_holder_ids, last_end_date_percent_rank
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (first_end_date_month_avg_distinct_holder_ids, last_end_date_percent_rank)
) unp
ORDER BY status, field

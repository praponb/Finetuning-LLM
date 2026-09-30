WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter,
        DATE_TRUNC('WEEK', end_date) AS end_date_week,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE start_date >= DATE '2024-12-02'
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.end_date_week,
        s.status_slug,
        p.segment AS holder_segment
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IN ('ACTIVE', 'LAPSED')
              AND p.start_date BETWEEN DATE '2024-07-19' AND DATE '2025-05-02'
        )
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', end_date_week) AS end_date_week_quarter,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        YEAR(end_date_week) AS end_date_week_year
    FROM screened
),
grouped AS (
    -- Summarise per group
    SELECT
        product,
        DATE_FORMAT(end_date_week_quarter, 'yyyy-MM') AS end_date_week_quarter_ym,
        holder_segment,
        COUNT(*) FILTER (WHERE status <> 'ACTIVE') AS n_status_active,
        MAX(end_date_week_year) AS max_end_date_week_year,
        ARRAY_SORT(COLLECT_SET(holder_segment)) AS holder_segment_list
    FROM annotated
    GROUP BY product, DATE_FORMAT(end_date_week_quarter, 'yyyy-MM'), holder_segment
    HAVING AVG(end_date_week_year) > 2025
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE max_end_date_week_year > (SELECT MIN(max_end_date_week_year) FROM grouped)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        product,
        end_date_week_quarter_ym,
        holder_segment,
        max_end_date_week_year,
        holder_segment_list,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_status_active ORDER BY n_status_active), 3) AS n_status_active_percent_rank,
        COUNT(*) OVER (PARTITION BY n_status_active, product) AS rows_in_n_status_active_product,
        LEAD(max_end_date_week_year) OVER w_all_by_max_end_date_week_yea AS next_max_end_date_week_year,
        DENSE_RANK() OVER (PARTITION BY holder_segment, n_status_active ORDER BY max_end_date_week_year DESC) AS max_end_date_week_year_dense_rank,
        LEAD(max_end_date_week_year, 1, 0) OVER (PARTITION BY holder_segment, n_status_active ORDER BY max_end_date_week_year DESC, product, end_date_week_quarter_ym, holder_segment) AS next_max_end_date_week_year_2
    FROM qualified
    WINDOW w_all_by_max_end_date_week_yea AS (ORDER BY max_end_date_week_year, product, end_date_week_quarter_ym, holder_segment)
)
SELECT product, end_date_week_quarter_ym, field, amount
FROM (
    SELECT product, end_date_week_quarter_ym, rows_in_n_status_active_product, next_max_end_date_week_year, max_end_date_week_year, next_max_end_date_week_year_2
    FROM ranked
) src
UNPIVOT (
    amount FOR field IN (rows_in_n_status_active_product AS m_rows_in_n_status_active_product, next_max_end_date_week_year AS m_next_max_end_date_week_year, max_end_date_week_year AS m_max_end_date_week_year, next_max_end_date_week_year_2 AS next_max_end_date_week_year_2_v)
) unp
ORDER BY product, end_date_week_quarter_ym, field

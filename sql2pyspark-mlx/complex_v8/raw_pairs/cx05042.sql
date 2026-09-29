WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        DATE_TRUNC('WEEK', end_date) AS end_date_week,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM policies
    WHERE (premium < 4670.9 OR status = 'LAPSED')
),
unique_rows AS (
    -- Keep the earliest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        end_date_week,
        days_since_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY days_since_start_date, id) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        end_date_week,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY days_since_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY product), 2) AS days_since_start_date_vs_product_avg,
        RANK() OVER w_all_by_days_since_start_date_2 AS days_since_start_date_rank,
        MIN(days_since_start_date) OVER (PARTITION BY product) AS product_min_days_since_start_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY product, YEAR(end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date
    FROM unique_rows
    WINDOW w_all_by_days_since_start_date_2 AS (ORDER BY days_since_start_date, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        end_date_week,
        last_known_start_date,
        rows_in_product,
        days_since_start_date_vs_product_avg,
        product_min_days_since_start_date,
        second_end_date,
        ADD_MONTHS(last_known_start_date, 6) AS last_known_start_date_plus_6m,
        COALESCE(rows_in_product, 0) AS rows_in_product_filled,
        DATE_TRUNC('QUARTER', last_known_start_date) AS last_known_start_date_quarter
    FROM ranked
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(rows_in_product_filled) OVER (PARTITION BY YEAR(second_end_date)) AS second_end_date_year_min_rows_in_product_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY product, holder_id ORDER BY end_date_week ASC), 3) AS end_date_week_cume_dist
    FROM enriched
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status = 'APPROVED'
        )
      AND s.holder_id IS NOT NULL
)
SELECT
    id,
    holder_id,
    product,
    end_date_week,
    last_known_start_date,
    rows_in_product,
    days_since_start_date_vs_product_avg,
    product_min_days_since_start_date,
    last_known_start_date_plus_6m,
    rows_in_product_filled,
    last_known_start_date_quarter
FROM screened
WHERE second_end_date_year_min_rows_in_product_filled <= 150
ORDER BY days_since_start_date_vs_product_avg NULLS LAST

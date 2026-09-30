WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        UPPER(TRIM(product)) AS product_clean
    FROM policies
    WHERE holder_id <= 15
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY holder_id, status ORDER BY start_date, id) AS start_date_dense_rank,
        MAX(premium) OVER w_start_date_year AS start_date_year_max_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date, id) AS seq_no
    FROM scoped
    WINDOW w_start_date_year AS (PARTITION BY YEAR(start_date))
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        seq_no,
        SUM(seq_no) AS total_seq_no,
        DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
        ROUND(AVG(start_date_year_max_premium), 2) AS avg_start_date_year_max_premium,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT status) AS distinct_status
    FROM with_trends
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
)
SELECT
    status,
    seq_no,
    total_seq_no,
    avg_start_date_year_max_premium,
    n_rows,
    distinct_status + n_rows AS distinct_status_plus_n_rows
FROM stats
ORDER BY avg_start_date_year_max_premium DESC NULLS FIRST, total_seq_no DESC, status, seq_no
LIMIT 10

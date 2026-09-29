WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE start_date IS NOT NULL
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        status,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) AS row_count,
        MIN(start_date) AS first_start_date
    FROM recent_policys
    GROUP BY DATE_TRUNC('MONTH', start_date), DATE_TRUNC('MONTH', end_date), status
),
scored AS (
    -- Derive helper columns
    SELECT
        start_date_month,
        end_date_month,
        status,
        row_count,
        DATE_FORMAT(first_start_date, 'MMM yyyy') AS first_start_date_label,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix,
        DATEDIFF(DATE '2025-06-30', first_start_date) AS days_since_first_start_date
    FROM summary
    WHERE status <> 'CANCELLED'
)
SELECT
    start_date_month,
    row_count,
    status_prefix,
    CASE WHEN row_count >= 9 THEN 'A' WHEN row_count >= 5 THEN 'B' ELSE 'C' END AS row_count_tier,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM scored
ORDER BY status_prefix, start_date_month DESC

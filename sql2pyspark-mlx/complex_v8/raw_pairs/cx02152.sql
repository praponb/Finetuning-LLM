WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
      AND status IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label,
        CONCAT_WS('-', status, status_clean) AS status_status_clean_label
    FROM base
    WHERE UPPER(TRIM(status_clean)) LIKE 'E%'
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    status_clean,
    start_date_label,
    status_status_clean_label
FROM enriched
ORDER BY start_date_label NULLS LAST

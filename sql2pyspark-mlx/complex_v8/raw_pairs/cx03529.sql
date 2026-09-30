WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date
    FROM policies
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM cleaned
)
SELECT
    id,
    holder_id,
    premium,
    start_date_quarter,
    DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label,
    DATE_ADD(start_date, 365) AS start_date_plus_365d
FROM enriched
ORDER BY holder_id DESC, start_date_plus_365d DESC, id
LIMIT 5

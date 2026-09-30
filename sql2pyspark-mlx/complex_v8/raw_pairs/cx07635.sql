WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        INITCAP(TRIM(status)) AS status_clean
    FROM policies
    WHERE premium < 3521.8
),
scored AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM valid
)
SELECT *
FROM (
    SELECT end_date, status, premium
    FROM scored
) src
PIVOT (
    MAX(premium)
    FOR status IN ('ACTIVE', 'CANCELLED', 'EXPIRED', 'LAPSED')
)
ORDER BY end_date

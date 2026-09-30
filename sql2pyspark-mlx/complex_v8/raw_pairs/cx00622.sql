WITH prepared_policys AS (
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
    WHERE YEAR(start_date) = 2025
      AND end_date < DATE '2026-04-25'
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM prepared_policys
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_start_date
    FROM derived s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT start_date, status, days_start_date_to_end_date
    FROM summarized_rows
) src
PIVOT (
    SUM(days_start_date_to_end_date)
    FOR status IN ('LAPSED', 'CANCELLED')
)

SELECT
    id,
    city,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) opened_date_row
FROM stores
WHERE opened_date >= '2025-10-10'

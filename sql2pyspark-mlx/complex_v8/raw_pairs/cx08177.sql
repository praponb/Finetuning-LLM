WITH filtered_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        birth_year,
        TRUNC(signup_date, 'MM') AS signup_date_month_start
    FROM listeners
    WHERE country <> 'UK'
),
all_events AS (
    -- Stack the branches
    SELECT
        'listener' AS src,
        id AS qty,
        signup_date_month_start AS day,
        country
    FROM filtered_listeners
    UNION ALL
    SELECT
        'listener_2' AS src,
        birth_year,
        signup_date,
        country
    FROM listeners
)
SELECT src, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(day) AS latest_day
FROM all_events
GROUP BY src
ORDER BY src

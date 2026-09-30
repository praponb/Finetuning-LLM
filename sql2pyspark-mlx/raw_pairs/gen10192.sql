WITH cleaned AS (
    SELECT
        *,
        LENGTH(name) AS name_length,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM customers
    WHERE city IS NOT NULL
),
latest_match AS (
    SELECT s.*, x.account_opened_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date > '2024-07-01'
        )
),
with_windows AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        age,
        name_length,
        city_clean,
        account_opened_date,
        LEAD(name_length) OVER w_grp AS next_name_length,
        LAG(age) OVER w_grp AS prev_age,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM kept
    WINDOW w_grp AS (PARTITION BY segment ORDER BY credit_limit, id)
),
daily AS (
    SELECT
        DATE_TRUNC('WEEK', account_opened_date) AS week_start,
        country,
        DATE_TRUNC('YEAR', account_opened_date) AS year_start,
        COUNT(*) AS num_rows,
        COUNT_IF(city = 'Singapore') AS singapore_events,
        SUM(CASE WHEN city = 'London' THEN age ELSE 0 END) AS london_age
    FROM with_windows
    GROUP BY 1, 2, 3
)
SELECT
    week_start,
    country,
    year_start,
    num_rows,
    singapore_events,
    CASE WHEN country RLIKE '^(JP|US)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(TH|SG)$' THEN 'GROUP_A' ELSE 'CORE' END AS country_zone
FROM daily;

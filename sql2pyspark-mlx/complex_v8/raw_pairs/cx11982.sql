WITH combined AS (
    -- Stack the branches
    SELECT
        birth_year AS ref_id,
        country AS name,
        signup_date AS day,
        'listener' AS record_type
    FROM listeners
    WHERE country = 'UK'
      AND signup_date >= DATE '2024-08-02'
    UNION
    SELECT
        id,
        country,
        signup_date,
        'listener_2' AS record_type
    FROM listeners
    WHERE YEAR(signup_date) = 2024
)
SELECT record_type, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(day) AS latest_day
FROM combined
GROUP BY record_type
ORDER BY record_type

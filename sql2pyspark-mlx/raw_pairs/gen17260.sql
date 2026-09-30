WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        age - credit_limit AS age_minus_credit_limit,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM customers
    WHERE signup_date >= TO_DATE('2023-08-28')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        segment,
        age_minus_credit_limit
    FROM valid
) src
PIVOT (
    AVG(age_minus_credit_limit) AS total
    FOR segment IN ('Consumer', 'Corporate')
)

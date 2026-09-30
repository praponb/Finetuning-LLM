WITH filtered AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE age > 39
)
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY signup_date) AS latest_rank
    FROM filtered
) ranked
WHERE latest_rank = 1;

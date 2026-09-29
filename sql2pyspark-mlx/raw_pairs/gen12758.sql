WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        REPLACE(email, ' ', '_') AS email_slug,
        CONCAT_WS('-', email, country) AS email_country_key,
        COALESCE(city, 'N/A') AS city_filled
    FROM customers
    WHERE MONTH(signup_date) IN (1, 4, 12)
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC NULLS LAST, id) AS latest_rank
    FROM cleaned
) t
WHERE latest_rank = 1;

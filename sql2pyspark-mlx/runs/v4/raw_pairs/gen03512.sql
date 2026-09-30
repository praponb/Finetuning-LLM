WITH cleaned AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        CASE WHEN country IN ('UK', 'JP') THEN 'Tier 1' ELSE 'Other' END AS country_group,
        IF(is_active, 'Yes', 'No') AS is_active_label
    FROM customers
    WHERE signup_date > DATE '2024-04-14'
      AND segment IS NOT NULL
      AND credit_limit BETWEEN 9300 AND 15100
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC NULLS LAST, id) AS latest_rank
    FROM cleaned
) ranked
WHERE latest_rank = 1;

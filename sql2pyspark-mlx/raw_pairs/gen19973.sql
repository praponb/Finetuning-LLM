WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS latest_rank
    FROM customers
    QUALIFY latest_rank = 1
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    signup_date,
    age,
    CASE WHEN city RLIKE '^(Tokyo|Chiang Mai)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS city_zone
FROM deduped;

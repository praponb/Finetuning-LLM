WITH cleaned AS (
    SELECT
        *,
        UPPER(store_name) store_name_upper,
        ROUND(sqft, 1) sqft_rounded
    FROM stores
    WHERE sqft <= 15492
)
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft_rounded
    FROM cleaned
) src
PIVOT (
    AVG(sqft_rounded) AS value_sum,
    COUNT(1) AS cnt
    FOR region IN ('South', 'North')
)

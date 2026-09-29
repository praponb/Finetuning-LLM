WITH deduped AS (
    SELECT
        id,
        store_name,
        opened_date,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date ASC NULLS FIRST, id ASC) AS rn
        FROM stores
    ) x
    WHERE rn = 1
)
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM deduped
GROUP BY year_start, week_start
HAVING COUNT(DISTINCT id) > 1;

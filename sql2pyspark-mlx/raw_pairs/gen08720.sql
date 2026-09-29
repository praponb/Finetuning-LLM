WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rank_in_group
    FROM stores
    QUALIFY rank_in_group = 1
)
SELECT
    city,
    region,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS sqft_per_south,
    AVG(sqft) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_avg_sqft,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY AVG(sqft) FILTER (WHERE city = 'Chiang Mai') DESC) AS chiang_mai_avg_sqft_percent_rank
FROM ranked
GROUP BY city, region, DATE_TRUNC('WEEK', opened_date)
HAVING sqft_per_south > 0
ORDER BY week_start NULLS LAST;

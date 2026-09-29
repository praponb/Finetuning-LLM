SELECT
    *,
    SUM(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
    SUM(sqft) OVER (PARTITION BY city) AS city_sum_sqft
FROM stores;

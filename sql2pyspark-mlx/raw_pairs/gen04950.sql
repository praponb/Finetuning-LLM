SELECT
    id,
    region,
    sqft,
    LEAD(sqft, 1, 0) OVER (ORDER BY opened_date, id) AS next_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) AS sqft_share,
    LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY sqft, id) AS next_sqft_2,
    SUM(sqft) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
    AVG(sqft) OVER (PARTITION BY MONTH(opened_date) ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6
FROM stores

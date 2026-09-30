SELECT
    id,
    opened_date,
    region,
    city,
    sqft,
    COUNT(*) OVER (PARTITION BY YEAR(opened_date), region) opened_date_year_count,
    LAST_VALUE(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_sqft
FROM stores
ORDER BY last_sqft;

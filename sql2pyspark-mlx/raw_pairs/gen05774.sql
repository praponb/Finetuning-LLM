WITH with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST) AS sqft_percent_rank,
        PERCENT_RANK() OVER (ORDER BY sqft DESC) AS sqft_percent_rank_2,
        SUM(sqft) OVER (PARTITION BY YEAR(opened_date), city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM stores
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    sqft_percent_rank,
    cumulative_sqft
FROM with_windows
ORDER BY region;

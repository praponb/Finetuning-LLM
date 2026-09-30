WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(sqft * 1.07, 2) AS sqft_adjusted,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE MONTH(opened_date) IN (3, 7, 8)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        sqft_adjusted,
        region_clean,
        PERCENT_RANK() OVER w_running AS sqft_percent_rank,
        LAST_VALUE(sqft_adjusted) IGNORE NULLS OVER w_recent AS last_known_sqft_adjusted
    FROM base
    WINDOW w_running AS (PARTITION BY city ORDER BY sqft ASC NULLS FIRST), w_recent AS (PARTITION BY region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY sqft_adjusted ASC, id ASC) AS event_seq,
        MIN(last_known_sqft_adjusted) OVER (PARTITION BY city) AS city_min_last_known_sqft_adjusted,
        NTILE(4) OVER (PARTITION BY region ORDER BY last_known_sqft_adjusted DESC, id ASC) AS last_known_sqft_adjusted_bucket,
        AVG(sqft) OVER (PARTITION BY region) AS region_avg_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) AS sqft_share
    FROM scored
    WHERE sqft BETWEEN 8269 AND 8583
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft_share DESC, id ASC) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
)
SELECT
    id,
    store_name,
    sqft,
    sqft_adjusted,
    region_clean,
    sqft_percent_rank,
    event_seq,
    last_known_sqft_adjusted_bucket,
    sqft_share,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Mall Central)$' THEN 'EXTENDED' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS store_name_zone
FROM latest
ORDER BY last_known_sqft_adjusted_bucket DESC, id DESC;

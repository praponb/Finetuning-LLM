WITH cleaned AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        UPPER(region) AS region_upper,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE city != 'New York'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date ASC, id ASC) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
eligible AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date <= s.opened_date
        )
),
ranked_stores AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY latest_rank ASC, id ASC) AS latest_rank_2
        FROM eligible
    ) x
    WHERE latest_rank_2 = 1
),
cleaned_stores AS (
    SELECT
        *,
        LOWER(city) AS city_lower,
        COALESCE(region, 'NONE') AS region_filled_2
    FROM ranked_stores
    WHERE region IS NOT NULL
      AND region NOT IN ('North', 'South')
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    region_filled,
    region_filled_2,
    LEAD(sqft, 2) OVER (ORDER BY opened_date ASC, id ASC) AS next_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY MONTH(opened_date), region), 4) AS sqft_share,
    FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
    LAG(sqft, 2) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), city ORDER BY opened_date ASC, id ASC) AS prev_sqft
FROM cleaned_stores;

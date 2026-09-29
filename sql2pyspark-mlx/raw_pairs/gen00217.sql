WITH base AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        INITCAP(TRIM(store_name)) AS store_name_title,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE store_name IS NOT NULL
),
remaining_keys AS (
    SELECT city, region
    FROM base
    EXCEPT
    SELECT city, region
    FROM stores
    WHERE MONTH(opened_date) IN (4, 9, 12)
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.region = k.region
),
valid AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        store_name_title,
        resolved_region,
        CASE WHEN region IN ('North', 'East') THEN 'Tier 1' ELSE 'Other' END AS region_group
    FROM verified
    WHERE opened_date > TO_DATE('2023-04-30')
      AND city <> 'Berlin'
),
metrics AS (
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share_2
    FROM valid
)
SELECT
    region,
    kpi,
    val
FROM (
    SELECT
        region,
        sqft_share,
        sqft_share_2
    FROM metrics
) src
UNPIVOT (
    val FOR kpi IN (sqft_share, sqft_share_2)
);

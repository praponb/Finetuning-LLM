WITH cleaned AS (
    SELECT
        *,
        UPPER(store_name) store_name_upper,
        CONCAT(region, ' - ', city) region_city,
        COALESCE(region, 'DIRECT') region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-06-07'
      AND region IS NOT NULL
),
enriched AS (
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id) next_sqft,
        LAG(sqft) OVER (PARTITION BY region, city ORDER BY opened_date, id) prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date, id) event_seq
    FROM cleaned
    WHERE region <> 'North'
),
not_flagged AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
matched_keys AS (
    SELECT city
    FROM not_flagged
    EXCEPT
    SELECT city
    FROM stores
    WHERE sqft <= 8667
),
verified AS (
    SELECT s.*
    FROM not_flagged s
    INNER JOIN matched_keys k ON s.city = k.city
),
eligible AS (
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock >= s.opened_date
        )
),
base AS (
    SELECT
        *,
        MONTH(opened_date) opened_date_month,
        DATE_FORMAT(opened_date, 'yyyy-MM') opened_date_label,
        COALESCE(region, 'NONE') region_clean
    FROM eligible
    WHERE opened_date >= DATE '2023-03-16'
      AND region IS NOT NULL
),
not_flagged_stores AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.amount >= 1450
        )
)
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    region,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(SUM(next_sqft) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS next_sqft_per_north,
    GROUPING(DATE_TRUNC('YEAR', opened_date)) AS gid
FROM not_flagged_stores
GROUP BY CUBE (DATE_TRUNC('YEAR', opened_date), region)
ORDER BY num_cities DESC;

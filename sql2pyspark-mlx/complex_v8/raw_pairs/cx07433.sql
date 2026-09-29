WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        CASE WHEN capacity >= 11768 THEN 'A' WHEN capacity >= 4912 THEN 'B' ELSE 'C' END AS capacity_tier,
        LENGTH(TRIM(city)) AS city_len
    FROM warehouses
    WHERE capacity <= 11506
      AND city NOT IN ('Bangkok', 'Singapore')
),
deduped AS (
    -- Keep the latest row per capacity_tier
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        capacity_tier,
        city_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY capacity_tier ORDER BY city_len DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
)
SELECT
    capacity_tier,
    city_len,
    GROUPING(capacity_tier) AS is_total_capacity_tier,
    ROUND(AVG(capacity), 2) AS avg_capacity,
    SUM(CASE WHEN UPPER(TRIM(wh_code)) LIKE 'B%' THEN 1 ELSE 0 END) AS n_upper_trim_wh_code_like_b,
    ARRAY_SORT(COLLECT_SET(wh_code)) AS wh_code_list
FROM deduped
GROUP BY GROUPING SETS ((capacity_tier, city_len), (capacity_tier), (city_len))
ORDER BY capacity_tier NULLS LAST, city_len NULLS LAST

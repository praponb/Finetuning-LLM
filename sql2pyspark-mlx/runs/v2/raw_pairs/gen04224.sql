WITH base AS (
    SELECT *
    FROM customers
    WHERE country != 'UK' AND LENGTH(name) >= 12
),
agg AS (
    SELECT is_active, ROUND(STDDEV(age), 2) AS stddev_age, SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM base
    GROUP BY is_active
)
SELECT *
FROM agg
ORDER BY stddev_age, is_active
LIMIT 5

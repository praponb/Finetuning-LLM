WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM suppliers
    EXCEPT ALL
    SELECT id, city
    FROM suppliers
    WHERE LENGTH(supplier_name) < 5
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(rating) OVER (PARTITION BY city, country) city_max_rating
    FROM reconciled
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost <= 700
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.brand NOT IN ('Stark', 'Globex')
        )
),
not_flagged_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.category = 'Sports'
        )
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rating >= 4.1 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
        COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        AVG(rating) AS avg_rating
    FROM not_flagged_suppliers
    GROUP BY CASE WHEN rating >= 4.1 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT rating_tier, jp_count
FROM daily
ORDER BY jp_count DESC NULLS FIRST;

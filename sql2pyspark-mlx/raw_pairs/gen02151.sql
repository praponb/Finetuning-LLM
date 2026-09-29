WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT_WS('/', country, city) country_city_key
    FROM suppliers
    WHERE city IS NOT NULL
      AND supplier_name IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(rating) OVER (PARTITION BY city, country ORDER BY rating, id) next_rating,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) event_seq
    FROM filtered
    WHERE city = 'Bangkok'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.category IN ('Books', 'Fashion')
        )
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM eligible
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE supplier_name LIKE '%Euro%'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id AND s.city = k.city
)
-- Multi-level aggregation
SELECT
    country,
    ROUND(AVG(rating), 2) AS avg_rating,
    PERCENTILE_APPROX(next_rating, ARRAY(0.25, 0.5, 0.75)) AS next_rating_quartiles,
    SUM(event_seq) FILTER (WHERE country = 'SG') AS sg_total_event_seq,
    SUM(next_rating) FILTER (WHERE country = 'JP') AS jp_total_next_rating,
    COUNT(*) AS num_rows
FROM reconciled
GROUP BY country

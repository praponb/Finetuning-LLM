WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost BETWEEN 540 AND 570
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        rating,
        AVG(rating) OVER w_recent AS last7_avg_rating,
        COUNT(rating) OVER w_recent AS last7_count_rating,
        MAX(rating) OVER w_recent AS last7_max_rating
    FROM eligible
    WHERE country IN ('DE', 'US')
    WINDOW w_recent AS (PARTITION BY city, country ORDER BY rating ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
metrics_suppliers AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        rating,
        last7_avg_rating,
        last7_count_rating,
        MAX(last7_avg_rating) OVER (PARTITION BY country) AS country_max_last7_avg_rating,
        NTILE(5) OVER (PARTITION BY country ORDER BY rating DESC, id ASC) AS rating_bucket
    FROM metrics
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY country_max_last7_avg_rating ASC, id ASC) AS rank_in_group
        FROM metrics_suppliers
    ) ranked
    WHERE rank_in_group = 1
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND LENGTH(ch.product_name) < 14
        )
)
-- Aggregate with conditional metrics
SELECT
    country,
    CASE WHEN rating_bucket >= 6 THEN 'LARGE' ELSE 'SMALL' END AS rating_bucket_tier,
    SUM(CASE WHEN country = 'TH' THEN last7_avg_rating ELSE 0 END) AS th_last7_avg_rating,
    ROUND(SUM(country_max_last7_avg_rating) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) AS country_max_last7_avg_rating_per_us,
    SUM(CASE WHEN country = 'US' THEN country_max_last7_avg_rating ELSE 0 END) AS us_country_max_last7_avg_rating,
    COUNT_IF(country = 'UK') AS uk_events,
    PERCENTILE_APPROX(last7_avg_rating, ARRAY(0.25, 0.5, 0.75)) AS last7_avg_rating_quartiles,
    MIN(last7_count_rating) AS min_last7_count_rating
FROM eligible_suppliers
GROUP BY ROLLUP (country, CASE WHEN rating_bucket >= 6 THEN 'LARGE' ELSE 'SMALL' END)
HAVING SUM(CASE WHEN country = 'TH' THEN last7_avg_rating ELSE 0 END) < 1000000
ORDER BY country_max_last7_avg_rating_per_us DESC NULLS FIRST

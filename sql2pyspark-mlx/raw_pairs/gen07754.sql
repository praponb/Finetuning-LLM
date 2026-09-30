WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior,
        MAX(rating) OVER (PARTITION BY country) AS country_max_rating,
        AVG(rating) OVER w_hist AS rating_avg_5,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS rating_percent_rank,
        STDDEV_SAMP(rating) OVER (PARTITION BY country, city) AS country_stddev_samp_rating
    FROM suppliers
    WINDOW w_hist AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category != 'Home'
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(supplier_name) AS clean_supplier_name
    FROM not_flagged
    WHERE city IS NOT NULL
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.rating_avg_2_prior,
        s.country_max_rating,
        s.rating_avg_5,
        s.clean_supplier_name,
        latest.cost
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched_suppliers AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        rating,
        rating_avg_2_prior,
        clean_supplier_name,
        cost,
        FIRST_VALUE(rating_avg_5, TRUE) OVER (PARTITION BY country ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating_avg_5,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY cost DESC, id) AS event_seq
    FROM latest_match
),
not_flagged_suppliers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NULL
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS rating_per_london,
        SUM(CASE WHEN country = 'US' THEN rating ELSE 0 END) AS us_rating,
        COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows
    FROM not_flagged_suppliers
    GROUP BY 1
)
SELECT
    city,
    rating_per_london,
    bangkok_rows,
    CASE WHEN city RLIKE '^(London|Chiang Mai)$' THEN 'OTHER_ZONE' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'CORE' ELSE 'GROUP_A' END AS city_zone
FROM stats
ORDER BY rating_per_london NULLS FIRST

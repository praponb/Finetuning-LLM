WITH valid AS (
    SELECT
        *,
        CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY country ORDER BY rating DESC, id ASC) AS rating_bucket,
        NTILE(10) OVER (PARTITION BY city ORDER BY rating DESC, id ASC) AS rating_bucket_2,
        NTILE(4) OVER (PARTITION BY city, country ORDER BY rating ASC, id ASC) AS rating_bucket_3,
        ROW_NUMBER() OVER w_hist AS row_seq,
        LEAD(is_bangkok, 2, 0) OVER w_hist AS next_is_bangkok
    FROM valid
    WINDOW w_hist AS (PARTITION BY city ORDER BY is_bangkok ASC, id ASC)
),
filtered AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_clean,
        rating_bucket,
        rating_bucket_2,
        rating_bucket_3,
        row_seq,
        next_is_bangkok,
        CEIL(is_bangkok) AS is_bangkok_whole,
        COALESCE(city, 'N/A') AS city_filled
    FROM with_windows
    WHERE city IS NOT NULL
      AND next_is_bangkok <= 59
),
enriched AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_clean,
        rating_bucket,
        rating_bucket_2,
        rating_bucket_3,
        row_seq,
        next_is_bangkok,
        is_bangkok_whole,
        city_filled,
        MIN(rating_bucket) OVER (PARTITION BY country) AS country_min_rating_bucket,
        RANK() OVER (ORDER BY is_bangkok_whole DESC) AS is_bangkok_whole_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_bucket_3 ASC, id ASC) AS event_seq
    FROM filtered
),
not_flagged AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND SIZE(x.tags) > 2
        )
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY next_is_bangkok ASC, id ASC) AS row_num
        FROM not_flagged
    ) x
    WHERE row_num = 1
),
valid_suppliers AS (
    SELECT
        *,
        CAST(next_is_bangkok AS STRING) AS next_is_bangkok_num,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM ranked
    WHERE country = 'DE'
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    city_clean,
    rating_bucket_2,
    rating_bucket_3,
    row_seq
FROM valid_suppliers;

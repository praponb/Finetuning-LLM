WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS FIRST, id) AS rank_in_group
        FROM suppliers
    ) t
    WHERE rank_in_group = 1
),
deduped_suppliers AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rank_in_group DESC, id) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
kept AS (
    SELECT s.*
    FROM deduped_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND MONTH(c.launch_date) IN (1, 5, 8)
        )
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        rank_in_group,
        rn,
        LAST_VALUE(rating, TRUE) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share,
        CUME_DIST() OVER (ORDER BY rating NULLS LAST) AS rating_cume_dist,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
    FROM kept
),
with_windows_suppliers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS event_seq,
        LEAD(rating_share, 1) OVER (PARTITION BY country ORDER BY rn, id) AS next_rating_share,
        FIRST_VALUE(rating_cume_dist, TRUE) OVER (PARTITION BY city ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating_cume_dist
    FROM with_windows
),
scored AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        rank_in_group,
        rn,
        last_known_rating,
        rating_share,
        rating_cume_dist,
        first_known_city,
        event_seq,
        next_rating_share,
        first_known_rating_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY first_known_rating_cume_dist, id) AS event_seq_2,
        SUM(rating_share) OVER (PARTITION BY country, city ORDER BY first_known_rating_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_share
    FROM with_windows_suppliers
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(rating_cume_dist, TRUE) OVER (PARTITION BY city, country ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_cume_dist,
        DENSE_RANK() OVER (PARTITION BY country, city ORDER BY first_known_rating_cume_dist NULLS LAST) AS first_known_rating_cume_dist_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY event_seq DESC, id) AS row_seq
    FROM scored
),
daily AS (
    SELECT
        country,
        CASE WHEN rating_share >= 0.69 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_share_tier,
        CASE WHEN rating_cume_dist >= 0.8 THEN 'HIGH' ELSE 'LOW' END AS rating_cume_dist_tier,
        MIN(cumulative_rating_share) AS min_cumulative_rating_share,
        COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
        COUNT(*) AS cnt,
        AVG(first_known_rating_cume_dist) FILTER (WHERE city = 'Tokyo') AS tokyo_avg_first_known_rating_cume_dist,
        COUNT(*) AS num_rows
    FROM enriched
    GROUP BY 1, 2, 3
    HAVING min_cumulative_rating_share < 0
)
SELECT
    country,
    rating_share_tier,
    rating_cume_dist_tier,
    min_cumulative_rating_share,
    cnt,
    tokyo_avg_first_known_rating_cume_dist,
    num_rows
FROM daily
ORDER BY cnt DESC NULLS LAST;

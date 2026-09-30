WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rank_in_group
    FROM suppliers
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(rating) OVER w_recent AS prev_rating,
        ROW_NUMBER() OVER w_recent AS event_seq,
        LAST_VALUE(rating, TRUE) OVER w_hist AS last_known_rating
    FROM latest
    WINDOW w_recent AS (PARTITION BY city, country ORDER BY rank_in_group, id), w_hist AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND SIZE(x.tags) >= 2
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY last_known_rating, id) AS dedup_rank
        FROM not_flagged
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(city) AS clean_city,
        COALESCE(city, 'NONE') AS city_filled
    FROM ranked
    WHERE last_known_rating >= 3.4
      AND city IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last_known_rating) OVER w_hist AS prev_last_known_rating,
        LEAD(prev_rating) OVER w_hist AS next_prev_rating,
        ROW_NUMBER() OVER w_hist AS event_seq_2,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM cleaned
    WHERE event_seq >= 4
    WINDOW w_hist AS (PARTITION BY country, city ORDER BY event_seq, id)
)
SELECT
    id,
    supplier_name,
    rating,
    rank_in_group,
    prev_rating,
    event_seq,
    last_known_rating,
    dedup_rank,
    city_filled,
    CASE WHEN supplier_name RLIKE '^(Tokyo Traders|Global Supply)$' THEN 'GROUP_B' WHEN supplier_name RLIKE '^(Siam Parts Co|EuroGoods GmbH)$' THEN 'EXTENDED' ELSE 'CORE' END AS supplier_name_zone
FROM scored
ORDER BY dedup_rank DESC, rating DESC;

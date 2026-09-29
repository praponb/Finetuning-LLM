WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(supplier_name) AS clean_supplier_name
    FROM suppliers
    WHERE UPPER(supplier_name) LIKE '%URO%'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        country,
        city,
        rating,
        clean_supplier_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS latest_rank
        FROM filtered
    ) ranked
    WHERE latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(rating) OVER w_running AS last4_min_rating,
        AVG(rating) OVER w_running AS last4_avg_rating,
        COUNT(rating) OVER w_running AS last4_count_rating,
        MAX(rating) OVER w_running AS last4_max_rating,
        LEAD(rating, 1) OVER w_grp AS next_rating
    FROM deduped
    WINDOW w_running AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY country, city ORDER BY rating, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        last4_max_rating,
        next_rating,
        LAG(last4_max_rating) OVER (PARTITION BY city ORDER BY last4_max_rating, id) AS prev_last4_max_rating,
        LEAD(last4_max_rating) OVER (PARTITION BY city ORDER BY last4_max_rating, id) AS next_last4_max_rating,
        LAG(last4_avg_rating) OVER (PARTITION BY city ORDER BY last4_max_rating, id) AS prev_last4_avg_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY last4_max_rating, id) AS row_seq
    FROM scored
),
filtered_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        last4_max_rating,
        next_rating,
        prev_last4_max_rating,
        prev_last4_avg_rating,
        ROUND(row_seq, 0) AS row_seq_rounded,
        row_seq * next_rating AS row_seq_times_next_rating
    FROM enriched
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Chiang Mai'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(next_rating) OVER (PARTITION BY city) AS city_count_next_rating,
        FIRST_VALUE(prev_last4_max_rating) IGNORE NULLS OVER (PARTITION BY city ORDER BY prev_last4_max_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_last4_max_rating,
        LEAD(row_seq_times_next_rating, 1) OVER (ORDER BY prev_last4_avg_rating, id) AS next_row_seq_times_next_rating
    FROM filtered_suppliers
)
-- Multi-level aggregation
SELECT
    city,
    CASE WHEN next_row_seq_times_next_rating >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_row_seq_times_next_rating_tier,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    COUNT(*) AS event_count,
    MIN(row_seq_rounded) AS min_row_seq_rounded
FROM with_windows
GROUP BY city, CASE WHEN next_row_seq_times_next_rating >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING min_row_seq_rounded > 0
ORDER BY city NULLS LAST

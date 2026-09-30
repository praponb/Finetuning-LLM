WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        UPPER(event_type) AS event_type_upper
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type <> 'search'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec ASC, id ASC) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec ASC, id ASC) AS event_seq,
        LAG(duration_sec, 2) OVER (PARTITION BY page, event_type ORDER BY duration_sec ASC, id ASC) AS prev_duration_sec_2
    FROM filtered
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        event_type_upper,
        prev_duration_sec,
        event_seq,
        prev_duration_sec_2,
        DATEDIFF(CURRENT_DATE(), event_time) / 30 AS months_since_event_time,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM enriched
    WHERE event_time >= TO_DATE('2023-07-12')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM cleaned
    EXCEPT
    SELECT id
    FROM web_events
    WHERE page = '/home'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY page, event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_cume_dist,
        MAX(prev_duration_sec_2) OVER (PARTITION BY page) AS page_max_prev_duration_sec_2,
        DENSE_RANK() OVER (ORDER BY months_since_event_time DESC NULLS FIRST) AS months_since_event_time_dense_rank
    FROM reconciled
    WHERE event_type = 'page_view'
)
-- Multi-level aggregation
SELECT
    event_type,
    CASE WHEN page_max_prev_duration_sec_2 >= 509 THEN 'LARGE' ELSE 'SMALL' END AS page_max_prev_duration_sec_2_tier,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(prev_duration_sec, 0.5) AS prev_duration_sec_p50,
    COUNT(*) AS event_count,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    RANK() OVER (PARTITION BY event_type ORDER BY ROUND(AVG(duration_sec), 2) DESC) AS avg_duration_sec_rank
FROM metrics
GROUP BY event_type, CASE WHEN page_max_prev_duration_sec_2 >= 509 THEN 'LARGE' ELSE 'SMALL' END
HAVING num_rows < 1000;

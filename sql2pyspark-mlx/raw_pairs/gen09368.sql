WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN duration_sec >= 424 THEN 'High' WHEN duration_sec >= 200 THEN 'Medium' ELSE 'Low' END AS duration_sec_band
    FROM web_events
    WHERE page IS DISTINCT FROM '/product'
      AND event_time >= DATE '2023-03-15'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        duration_sec_band,
        NTILE(10) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket,
        CUME_DIST() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_cume_dist,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_5_prior,
        LAG(duration_sec, 1) OVER (PARTITION BY page ORDER BY duration_sec, id) AS prev_duration_sec,
        RANK() OVER (ORDER BY duration_sec NULLS LAST) AS duration_sec_rank
    FROM valid
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_rank DESC, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT a.*, d.browser
    FROM deduped a
    LEFT JOIN sessions d
        ON a.session_id = d.id
    WHERE d.country = 'TH'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY duration_sec_cume_dist, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY event_type ORDER BY duration_sec_avg_5_prior DESC, id) AS duration_sec_avg_5_prior_bucket,
        LAG(duration_sec_bucket, 1) OVER (PARTITION BY event_type ORDER BY rank_in_group, id) AS prev_duration_sec_bucket,
        RANK() OVER (PARTITION BY session_id ORDER BY duration_sec NULLS FIRST) AS duration_sec_rank_2,
        SUM(duration_sec_bucket) OVER (ORDER BY prev_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_bucket
    FROM with_dims
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            browser,
            event_type,
            duration_sec_rank_2
        FROM metrics
    ) src
    PIVOT (
        MAX(duration_sec_rank_2)
        FOR event_type IN ('click', 'add_to_cart', 'search')
    )
)
SELECT
    browser,
    click,
    add_to_cart,
    CASE WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'GROUP_A' WHEN browser RLIKE '^(Safari|Firefox)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS browser_zone
FROM by_type
ORDER BY click DESC NULLS FIRST;

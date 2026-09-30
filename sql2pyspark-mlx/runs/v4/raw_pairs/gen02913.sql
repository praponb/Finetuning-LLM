WITH valid AS (
    SELECT *, ROUND(duration_sec, 2) AS duration_sec_rounded
    FROM web_events
    WHERE page IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY duration_sec DESC, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    SELECT
        *,
        MAX(duration_sec_rounded) OVER (PARTITION BY session_id, event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_rounded_max_7d,
        NTILE(4) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket,
        SUM(duration_sec_rounded) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_rounded,
        NTILE(5) OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2
    FROM deduped
),
with_windows_web_events AS (
    SELECT
        *,
        AVG(duration_sec_rounded_max_7d) OVER (PARTITION BY session_id ORDER BY duration_sec_bucket, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_rounded_max_7d_avg_2,
        PERCENT_RANK() OVER w_part AS duration_sec_bucket_percent_rank,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS event_seq
    FROM with_windows
    WINDOW w_part AS (PARTITION BY event_type ORDER BY duration_sec_bucket DESC NULLS FIRST)
),
with_dims AS (
    SELECT
        e.*,
        d.device,
        d.country
    FROM with_windows_web_events e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
base AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        rn,
        duration_sec_rounded_max_7d,
        duration_sec_bucket,
        cumulative_duration_sec_rounded,
        duration_sec_rounded_max_7d_avg_2,
        event_seq,
        country,
        CASE WHEN cumulative_duration_sec_rounded >= 412 THEN 'High' WHEN cumulative_duration_sec_rounded >= 66 THEN 'Medium' ELSE 'Low' END AS cumulative_duration_sec_rounded_level,
        INITCAP(TRIM(page)) AS page_title
    FROM with_dims
    WHERE event_time > DATE '2024-01-29'
      AND country IS NOT NULL
      AND cumulative_duration_sec_rounded BETWEEN 167 AND 320
),
scored AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY country ORDER BY duration_sec_rounded_max_7d_avg_2, id) AS duration_sec_rounded_max_7d_avg_2_bucket,
        CUME_DIST() OVER (PARTITION BY country ORDER BY cumulative_duration_sec_rounded DESC NULLS FIRST) AS cumulative_duration_sec_rounded_cume_dist,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY duration_sec DESC) AS duration_sec_dense_rank
    FROM base
)
SELECT
    CASE WHEN cumulative_duration_sec_rounded >= 325 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_duration_sec_rounded_tier,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    ROUND(AVG(duration_sec_bucket), 2) AS avg_duration_sec_bucket,
    MAX(duration_sec_dense_rank) FILTER (WHERE country = 'DE') AS de_max_duration_sec_dense_rank
FROM scored
GROUP BY CASE WHEN cumulative_duration_sec_rounded >= 325 THEN 'PREMIUM' ELSE 'STANDARD' END

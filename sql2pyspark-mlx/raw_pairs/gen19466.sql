WITH filtered AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        LENGTH(page) AS page_length,
        SUBSTRING(page, 1, 2) AS page_prefix
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-03-30 03:00:00'
      AND page IS NOT NULL
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id ASC) = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY page_length DESC NULLS FIRST, id ASC) AS rn
    FROM deduped
    QUALIFY rn = 1
),
with_windows AS (
    SELECT
        id,
        page_prefix,
        rn,
        SUM(page_length) OVER (PARTITION BY session_id ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_page_length,
        AVG(page_length) OVER (PARTITION BY session_id ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_page_length,
        MIN(page_length) OVER (PARTITION BY session_id ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_page_length,
        COUNT(page_length) OVER (PARTITION BY session_id ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_page_length
    FROM ranked
),
totals AS (
    SELECT
        CASE WHEN running_count_page_length >= 13 THEN 'LARGE' ELSE 'SMALL' END AS running_count_page_length_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS num_rows,
        ROUND(AVG(running_avg_page_length), 2) AS avg_running_avg_page_length,
        ROUND(AVG(running_count_page_length), 2) AS avg_running_count_page_length,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM with_windows
    GROUP BY CASE WHEN running_count_page_length >= 13 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    running_count_page_length_tier,
    distinct_ids,
    num_rows,
    avg_running_count_page_length,
    distinct_ids_2
FROM totals
ORDER BY distinct_ids_2 ASC NULLS LAST

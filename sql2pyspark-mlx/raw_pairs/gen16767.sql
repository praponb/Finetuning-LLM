WITH ranked AS (
    SELECT
        id,
        event_time,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec NULLS LAST, id) AS rank_in_group
        FROM web_events
    ) x
    WHERE rank_in_group = 1
),
with_windows AS (
    SELECT
        *,
        SUM(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_duration_sec,
        COUNT(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_duration_sec,
        MAX(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_duration_sec
    FROM ranked
),
remaining_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id
    FROM web_events
    WHERE duration_sec >= 285
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
cleaned AS (
    SELECT
        event_time,
        duration_sec,
        running_count_duration_sec,
        CEIL(duration_sec) duration_sec_whole
    FROM reconciled
    WHERE event_time >= TO_DATE('2023-05-14')
),
valid AS (
    SELECT *, ROUND(running_count_duration_sec, 2) running_count_duration_sec_rounded
    FROM cleaned
    WHERE event_time >= TO_DATE('2023-05-26')
)
SELECT
    duration_sec,
    running_count_duration_sec,
    duration_sec_whole,
    MAX(duration_sec_whole) OVER w_ordered event_time_to_date_max_duration_sec_whole,
    CUME_DIST() OVER (PARTITION BY TO_DATE(event_time) ORDER BY duration_sec DESC NULLS FIRST) duration_sec_cume_dist,
    AVG(duration_sec_whole) OVER w_recent duration_sec_whole_avg_90d,
    SUM(running_count_duration_sec_rounded) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) running_count_duration_sec_rounded_sum_7d,
    PERCENT_RANK() OVER (ORDER BY running_count_duration_sec_rounded DESC NULLS LAST) running_count_duration_sec_rounded_percent_rank
FROM valid
WINDOW w_ordered AS (PARTITION BY TO_DATE(event_time)), w_recent AS (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)

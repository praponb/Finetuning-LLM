WITH filtered AS (
    SELECT *, CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-03-13 06:00:00'
),
enriched AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_time_day,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('YEAR', event_time_day) ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        PERCENT_RANK() OVER (PARTITION BY session_id, page ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_percent_rank
    FROM filtered
    WHERE event_time_day > '2023-11-23'
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY cumulative_duration_sec DESC, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec) AS duration_sec_cume_dist,
        SUM(duration_sec_percent_rank) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_percent_rank,
        NTILE(5) OVER (PARTITION BY YEAR(event_time_day), page ORDER BY duration_sec_percent_rank, id) AS duration_sec_percent_rank_bucket
    FROM enriched
),
enriched_web_events AS (
    SELECT
        *,
        AVG(duration_sec_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', event_time_day), page ORDER BY event_time_day, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_avg_6,
        LEAD(duration_sec_bucket, 1) OVER (PARTITION BY event_type ORDER BY duration_sec_avg_2_prior, id) AS next_duration_sec_bucket,
        RANK() OVER (PARTITION BY event_type ORDER BY cumulative_duration_sec DESC NULLS LAST) AS cumulative_duration_sec_rank,
        COUNT(duration_sec) OVER (PARTITION BY event_type, page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d
    FROM metrics
),
enriched_web_events_2 AS (
    SELECT
        *,
        COUNT(duration_sec_bucket) OVER (PARTITION BY DATE_TRUNC('week', event_time_day), event_type) AS event_time_day_period_count_duration_sec_bucket,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY next_duration_sec_bucket, id) AS event_seq,
        SUM(next_duration_sec_bucket) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS next_duration_sec_bucket_sum_7d
    FROM enriched_web_events
)
SELECT
    id,
    page,
    event_time,
    event_time_day,
    duration_sec_avg_2_prior,
    duration_sec_percent_rank,
    seq_no,
    duration_sec_cume_dist,
    duration_sec_percent_rank_bucket
FROM enriched_web_events_2;

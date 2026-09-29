WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        reading_ts,
        is_new_burst
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) AS dedup_rank
        FROM sessionized
    ) t
    WHERE dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        reading_ts,
        LAST_VALUE(is_new_burst, TRUE) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_is_new_burst,
        AVG(is_new_burst) OVER (ORDER BY is_new_burst, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) is_new_burst_avg_6
    FROM latest
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT
    SELECT id
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 8 AND 15
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (ORDER BY is_new_burst_avg_6 NULLS FIRST) is_new_burst_avg_6_rank
    FROM kept_rows
)
-- Ranking and gap features
SELECT
    last_known_is_new_burst,
    is_new_burst_avg_6,
    is_new_burst_avg_6_rank,
    PERCENT_RANK() OVER (ORDER BY last_known_is_new_burst DESC NULLS FIRST) last_known_is_new_burst_percent_rank,
    CUME_DIST() OVER (ORDER BY is_new_burst_avg_6_rank DESC NULLS FIRST) is_new_burst_avg_6_rank_cume_dist,
    DENSE_RANK() OVER (ORDER BY last_known_is_new_burst DESC NULLS LAST) last_known_is_new_burst_dense_rank,
    AVG(is_new_burst_avg_6_rank) OVER (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) is_new_burst_avg_6_rank_avg_30m,
    RANK() OVER (ORDER BY is_new_burst_avg_6 NULLS LAST) is_new_burst_avg_6_rank_2
FROM enriched;

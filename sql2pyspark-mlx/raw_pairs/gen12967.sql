WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id) next_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id) prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) row_seq,
        AVG(duration_sec) OVER (PARTITION BY session_id) session_id_avg_duration_sec
    FROM ranked
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(next_duration_sec, duration_sec) greatest_next_duration_sec_duration_sec,
        CASE WHEN next_duration_sec >= 481 THEN 'High' WHEN next_duration_sec >= 173 THEN 'Medium' ELSE 'Low' END next_duration_sec_tier
    FROM metrics
    WHERE row_seq BETWEEN 5 AND 6
),
metrics_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(prev_duration_sec, 2) OVER (ORDER BY greatest_next_duration_sec_duration_sec, id) next_prev_duration_sec,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) seq_no
    FROM cleaned
    WHERE greatest_next_duration_sec_duration_sec < 80
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics_web_events
    EXCEPT ALL
    SELECT id
    FROM web_events
    WHERE TO_DATE(event_time) < '2023-06-14'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_web_events s
    INNER JOIN kept_keys k ON s.id = k.id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM kept_rows
    EXCEPT
    SELECT id
    FROM web_events
    WHERE event_type = 'search'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys_2 k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        duration_sec,
        next_duration_sec,
        prev_duration_sec,
        row_seq,
        next_duration_sec_tier,
        next_prev_duration_sec,
        seq_no,
        duration_sec - greatest_next_duration_sec_duration_sec duration_sec_minus_greatest_next_duration_sec_duration_sec,
        CAST(duration_sec AS DOUBLE) duration_sec_str
    FROM reconciled
    WHERE row_seq <= 4
),
daily AS (
    -- Grouped metrics
    SELECT
        seq_no,
        BIT_OR(duration_sec) AS duration_sec_bit_or,
        BIT_OR(duration_sec) AS duration_sec_bit_or_2,
        COUNT(*) AS row_count,
        ROUND(AVG(duration_sec_str), 2) AS avg_duration_sec_str,
        BIT_OR(duration_sec) AS duration_sec_bit_or_3
    FROM filtered
    GROUP BY CUBE (seq_no)
    HAVING duration_sec_bit_or_3 > 1
)
SELECT
    duration_sec_bit_or,
    row_count,
    avg_duration_sec_str,
    duration_sec_bit_or_3
FROM daily
ORDER BY duration_sec_bit_or_3 NULLS LAST;

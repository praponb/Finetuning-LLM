WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        ADD_MONTHS(hired_on, 3) AS hired_on_plus_3m,
        DAYOFWEEK(hired_on) AS hired_on_dayofweek
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 7
      AND tier IN ('T3', 'T1')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        hired_on_dayofweek,
        ROUND(STDDEV(hired_on_dayofweek), 2) AS stddev_hired_on_dayofweek,
        COUNT(*) AS row_count,
        MIN(hired_on) AS first_hired_on,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        PERCENTILE_APPROX(hired_on_dayofweek, 0.5) AS median_hired_on_dayofweek
    FROM prepared
    WHERE hired_on_plus_3m < DATE '2025-07-01'
    GROUP BY hired_on_dayofweek
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        hired_on_dayofweek,
        stddev_hired_on_dayofweek,
        first_hired_on,
        median_hired_on_dayofweek,
        NTILE(10) OVER w_hired_on_dayofweek_by_row_co AS row_count_decile,
        COUNT(*) OVER (ORDER BY row_count DESC, hired_on_dayofweek ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM stats
    WINDOW w_hired_on_dayofweek_by_row_co AS (PARTITION BY hired_on_dayofweek ORDER BY row_count DESC, hired_on_dayofweek)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE stddev_hired_on_dayofweek > (SELECT AVG(stddev_hired_on_dayofweek) FROM with_windows)
)
SELECT
    hired_on_dayofweek,
    stddev_hired_on_dayofweek,
    first_hired_on,
    median_hired_on_dayofweek,
    row_count_decile
FROM with_activity
WHERE stddev_hired_on_dayofweek >= 1.1

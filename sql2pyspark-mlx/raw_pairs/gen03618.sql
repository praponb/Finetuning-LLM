WITH base AS (
    SELECT
        id,
        manager_id,
        hire_date,
        is_remote,
        level,
        CONCAT(full_name, '/', level) AS full_name_level,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM employees
    WHERE hire_date >= DATE '2023-08-23'
),
cleaned AS (
    SELECT
        id,
        manager_id,
        hire_date,
        is_remote,
        level,
        level_filled,
        DATE_TRUNC('YEAR', hire_date) AS hire_date_period
    FROM base
    WHERE manager_id IN (10, 22)
),
enriched AS (
    SELECT *, LAST_VALUE(level, TRUE) OVER (PARTITION BY manager_id ORDER BY hire_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM cleaned
),
with_gaps AS (
    SELECT *, CASE WHEN hire_date_period - LAG(hire_date_period) OVER (PARTITION BY level ORDER BY hire_date_period, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY level ORDER BY hire_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    level,
    session_no,
    manager_id,
    MIN(is_new_session) AS min_is_new_session,
    APPROX_PERCENTILE(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    APPROX_PERCENTILE(is_new_session, 0.9) AS is_new_session_p90,
    COUNT(*) AS row_count
FROM sessionized
GROUP BY level, session_no, manager_id
HAVING is_new_session_p90 >= 0
ORDER BY row_count DESC NULLS LAST

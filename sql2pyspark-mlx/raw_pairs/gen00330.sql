WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        salary,
        hire_date,
        is_remote,
        level,
        LENGTH(full_name) AS full_name_length
    FROM employees
    WHERE hire_date >= DATE '2023-04-13'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY level ORDER BY full_name_length DESC, id ASC) AS full_name_length_bucket,
        AVG(full_name_length) OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS full_name_length_avg_6
    FROM cleaned
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id ASC) AS rn
    FROM metrics
    QUALIFY rn = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM ranked
    EXCEPT
    SELECT level, id
    FROM employees
    WHERE dept_id IN (4, 13, 24)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.level = k.level AND s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        salary,
        full_name_length_bucket,
        full_name_length_avg_6,
        RANK() OVER (PARTITION BY level ORDER BY salary ASC) AS salary_rank,
        RANK() OVER w_running AS full_name_length_rank,
        AVG(full_name_length) OVER w_hist AS level_avg_full_name_length,
        CUME_DIST() OVER w_ordered AS salary_cume_dist
    FROM reconciled
    WINDOW w_running AS (PARTITION BY level ORDER BY full_name_length DESC NULLS LAST), w_hist AS (PARTITION BY level), w_ordered AS (PARTITION BY YEAR(hire_date), level ORDER BY salary ASC)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN full_name_length_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS full_name_length_rank_tier,
    PERCENTILE_APPROX(full_name_length_rank, 0.9) AS full_name_length_rank_p90,
    SUM(full_name_length_rank) AS total_full_name_length_rank
FROM enriched
GROUP BY CASE WHEN full_name_length_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END;

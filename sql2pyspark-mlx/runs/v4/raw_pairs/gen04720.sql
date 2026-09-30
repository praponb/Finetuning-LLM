WITH scored AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        CUME_DIST() OVER (PARTITION BY location ORDER BY budget ASC NULLS FIRST) AS budget_cume_dist,
        NTILE(5) OVER (ORDER BY budget DESC, id ASC) AS budget_bucket,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM departments
    WINDOW w_hist AS (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC)
),
valid AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        seq_no,
        budget_cume_dist + seq_no AS budget_cume_dist_plus_seq_no,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM scored
    WHERE location IS NOT NULL
),
kept_keys AS (
    SELECT id, location
    FROM valid
    EXCEPT ALL
    SELECT id, location
    FROM departments
    WHERE budget <= 2576000
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.id = k.id AND s.location = k.location
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary BETWEEN 63000 AND 87000
        )
),
not_flagged AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        FLOOR(seq_no) AS seq_no_whole,
        COALESCE(location, 'DIRECT') AS location_clean
    FROM not_flagged
    WHERE location IN ('New York', 'London')
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        seq_no,
        budget_cume_dist_plus_seq_no,
        seq_no_filled,
        location_title,
        seq_no_whole,
        location_clean,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY seq_no_filled DESC NULLS FIRST) AS seq_no_filled_cume_dist,
        AVG(seq_no_filled) OVER w_ordered AS dept_name_avg_seq_no_filled
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY dept_name)
),
summary AS (
    SELECT
        seq_no,
        CASE WHEN seq_no_whole >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_whole_tier,
        PERCENTILE_APPROX(budget_cume_dist_plus_seq_no, 0.95) AS budget_cume_dist_plus_seq_no_p95,
        SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
        ROUND(SUM(dept_name_avg_seq_no_filled) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS dept_name_avg_seq_no_filled_per_engineering,
        COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END) AS engineering_count,
        GROUPING(seq_no) AS grouping_id
    FROM enriched
    GROUP BY GROUPING SETS ((seq_no, CASE WHEN seq_no_whole >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END), (seq_no), ())
)
SELECT
    seq_no_whole_tier,
    dept_name_avg_seq_no_filled_per_engineering,
    engineering_count,
    grouping_id
FROM summary;

WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
    WHERE term IN ('2025T2', '2025T1')
      AND course_id >= 4
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_dayofweek,
        grade_status_clean,
        MAX(enrolled_at_dayofweek) OVER (PARTITION BY course_id, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_enrolled_at_dayofweek_so_far,
        SUM(enrolled_at_dayofweek) OVER w_student_id AS student_id_sum_enrolled_at_dayofweek,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        SUM(enrolled_at_dayofweek) OVER (PARTITION BY term, enrolled_at_dayofweek ORDER BY enrolled_at_dayofweek, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS enrolled_at_dayofweek_sum_6_prior,
        ROW_NUMBER() OVER (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY enrolled_at_dayofweek DESC NULLS FIRST, id) AS seq_no,
        MAX(enrolled_at_dayofweek) OVER (PARTITION BY term) AS term_max_enrolled_at_dayofweek,
        MAX(enrolled_at_dayofweek) OVER (PARTITION BY course_id, term ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS enrolled_at_dayofweek_max_7,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term, course_id)) AS distinct_grade_status_in_term_course_id,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY enrolled_at_dayofweek DESC NULLS FIRST), 3) AS enrolled_at_dayofweek_percent_rank,
        SUM(enrolled_at_dayofweek) OVER (ORDER BY enrolled_at_dayofweek ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS enrolled_at_dayofweek_sum_1_prior
    FROM recent_enrollments
    WINDOW w_student_id AS (PARTITION BY student_id), w_term_course_id AS (PARTITION BY term, course_id)
),
annotated AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM metrics
    WHERE rows_in_term BETWEEN 3 AND 18
),
per_group AS (
    -- Summarise per group
    SELECT
        seq_no,
        SUM(term_max_enrolled_at_dayofweek) AS total_term_max_enrolled_at_dayofweek,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        SUM(max_enrolled_at_dayofweek_so_far) AS total_max_enrolled_at_dayofweek_so_far
    FROM annotated
    GROUP BY seq_no
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        seq_no,
        total_term_max_enrolled_at_dayofweek,
        term_list,
        total_max_enrolled_at_dayofweek_so_far,
        seq_no - LAG(seq_no) OVER w_seq_no_by_total_max_enrolled AS seq_no_change,
        NTILE(3) OVER (PARTITION BY seq_no ORDER BY total_max_enrolled_at_dayofweek_so_far NULLS LAST, seq_no) AS total_max_enrolled_at_dayofweek_so_far_tercile
    FROM per_group
    WINDOW w_seq_no_by_total_max_enrolled AS (PARTITION BY seq_no ORDER BY total_max_enrolled_at_dayofweek_so_far NULLS LAST, seq_no)
)
-- Final projection
SELECT
    seq_no,
    total_term_max_enrolled_at_dayofweek,
    term_list,
    total_max_enrolled_at_dayofweek_so_far,
    seq_no_change,
    total_max_enrolled_at_dayofweek_so_far_tercile,
    ABS(seq_no - 7) AS seq_no_distance,
    FLOOR(seq_no_change / 5) * 5 AS seq_no_change_bucket
FROM metrics_2
WHERE seq_no >= 2
ORDER BY seq_no_distance NULLS LAST, seq_no

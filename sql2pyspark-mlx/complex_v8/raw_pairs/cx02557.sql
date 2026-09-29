WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        term_or_unknown,
        LAST_VALUE(days_since_enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_enrolled_at,
        SIZE(COLLECT_SET(course_id) OVER w_term) AS distinct_course_id_in_term,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term)) AS distinct_grade_status_in_term
    FROM valid
    WINDOW w_term AS (PARTITION BY term)
),
unique_rows AS (
    -- Keep the earliest row per student_id
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        term_or_unknown,
        last_known_days_since_enrolled_at,
        distinct_course_id_in_term,
        distinct_grade_status_in_term
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY last_known_days_since_enrolled_at, id) AS recency_rank
        FROM windowed
    ) ranked
    WHERE recency_rank = 1
),
student_id_stats AS (
    -- Max last_known_days_since_enrolled_at per student_id
    SELECT student_id, MAX(last_known_days_since_enrolled_at) AS max_last_known_days_since_enrolled_at
    FROM unique_rows
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_last_known_days_since_enrolled_at,
        ROUND(s.last_known_days_since_enrolled_at - g.max_last_known_days_since_enrolled_at, 2) AS last_known_days_since_enrolled_at_minus_max
    FROM unique_rows s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.last_known_days_since_enrolled_at = g.max_last_known_days_since_enrolled_at
),
summary AS (
    -- Summarise per group
    SELECT
        term,
        grade_status,
        TO_DATE(enrolled_at) AS enrolled_at_day,
        SUM(last_known_days_since_enrolled_at) AS total_last_known_days_since_enrolled_at,
        COUNT(*) AS n_rows,
        ROUND(SUM(last_known_days_since_enrolled_at_minus_max) / NULLIF(SUM(max_last_known_days_since_enrolled_at), 0), 3) AS last_known_days_since_enrolled_at_minus_max_to_m
    FROM above_average
    WHERE max_last_known_days_since_enrolled_at <= 385.0
    GROUP BY term, grade_status, TO_DATE(enrolled_at)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_n_rows AS rows_in_n_rows,
        LEAD(n_rows) OVER (PARTITION BY term ORDER BY total_last_known_days_since_enrolled_at DESC, term, grade_status, enrolled_at_day) AS next_n_rows,
        LAST_VALUE(n_rows) IGNORE NULLS OVER (PARTITION BY term ORDER BY n_rows DESC, term, grade_status, enrolled_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_rows,
        MAX(n_rows) OVER (PARTITION BY term ORDER BY total_last_known_days_since_enrolled_at DESC, term, grade_status, enrolled_at_day ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS n_rows_max_2
    FROM summary
    WINDOW w_n_rows AS (PARTITION BY n_rows)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        term,
        grade_status,
        enrolled_at_day,
        total_last_known_days_since_enrolled_at,
        n_rows,
        last_known_days_since_enrolled_at_minus_max_to_m,
        rows_in_n_rows,
        next_n_rows,
        last_known_n_rows,
        ROUND(CUME_DIST() OVER (ORDER BY last_known_n_rows DESC NULLS FIRST), 3) AS last_known_n_rows_cume_dist,
        ROW_NUMBER() OVER (ORDER BY last_known_n_rows DESC NULLS FIRST, term, grade_status, enrolled_at_day) AS last_known_n_rows_seq,
        SUM(rows_in_n_rows) OVER (ORDER BY last_known_n_rows DESC NULLS FIRST, term, grade_status, enrolled_at_day ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS rows_in_n_rows_sum_1_prior,
        ROUND(n_rows_max_2 - AVG(n_rows_max_2) OVER w_last_known_n_rows_term, 2) AS n_rows_max_2_vs_last_known_n_rows_term_avg
    FROM ranked
    WINDOW w_last_known_n_rows_term AS (PARTITION BY last_known_n_rows, term)
)
-- Subtotals with CUBE
SELECT
    grade_status,
    last_known_n_rows_seq,
    SUM(CASE WHEN grade_status NOT IN ('PASS', 'FAIL') THEN 1 ELSE 0 END) AS n_grade_status_not_in_pass_fai,
    MAX(last_known_days_since_enrolled_at_minus_max_to_m) AS max_last_known_days_since_enrolled_at_minus_max_
FROM sequenced
GROUP BY CUBE(grade_status, last_known_n_rows_seq)
ORDER BY grade_status NULLS LAST, last_known_n_rows_seq NULLS LAST

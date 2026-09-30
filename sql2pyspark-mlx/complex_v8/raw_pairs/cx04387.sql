WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE term IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_enrolled_at)) / 60, 1) AS minutes_since_prev_enrolled_at,
        RANK() OVER w_course_id_enrolled_at_month AS enrolled_at_rank,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY course_id, MONTH(enrolled_at) ORDER BY enrolled_at, id) AS seq_no_2
    FROM cleaned_enrollments
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_course_id_enrolled_at_month AS (PARTITION BY course_id, MONTH(enrolled_at) ORDER BY enrolled_at)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (ORDER BY seq_no ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        COUNT(*) OVER (PARTITION BY student_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(CUME_DIST() OVER (ORDER BY seq_no ASC), 3) AS seq_no_cume_dist,
        LEAD(seq_no_2, 3) OVER (ORDER BY seq_no ASC, id) AS next3_seq_no_2,
        SUM(seq_no_2) OVER (PARTITION BY grade_status, seq_no_2) AS grade_status_seq_no_2_sum_seq_no_2,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, term ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist
    FROM sequenced
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE seq_no_2 <= 5
),
totals AS (
    -- Group-level totals
    SELECT
        last_known_seq_no,
        student_id,
        ROUND(SUM(next3_seq_no_2) / NULLIF(SUM(grade_status_seq_no_2_sum_seq_no_2), 0), 3) AS next3_seq_no_2_to_grade_status_seq_no_2_sum_seq_,
        SUM(CASE WHEN grade_status NOT IN ('PASS', 'WITHDRAWN') THEN 1 ELSE 0 END) AS n_grade_status_not_in_pass_wit,
        ROUND(SUM(minutes_since_prev_enrolled_at), 2) AS total_minutes_since_prev_enrolled_at,
        COUNT(*) FILTER (WHERE term <> '2025T1') AS n_term_2025t1
    FROM first_rows
    GROUP BY 1, 2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        last_known_seq_no,
        student_id,
        n_grade_status_not_in_pass_wit,
        n_term_2025t1,
        ROUND(PERCENT_RANK() OVER (ORDER BY total_minutes_since_prev_enrolled_at DESC), 3) AS total_minutes_since_prev_enrolled_at_percent_ran,
        LAG(n_term_2025t1) OVER (ORDER BY total_minutes_since_prev_enrolled_at, last_known_seq_no, student_id) AS prev_n_term_2025t1
    FROM totals
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE prev_n_term_2025t1 < (SELECT MAX(prev_n_term_2025t1) FROM with_windows)
)
SELECT
    last_known_seq_no,
    student_id,
    n_term_2025t1,
    ROUND(n_term_2025t1 / NULLIF(prev_n_term_2025t1, 0), 3) AS n_term_2025t1_per_prev_n_term_2025t1
FROM screened
ORDER BY student_id DESC, last_known_seq_no DESC

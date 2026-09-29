WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
    WHERE course_id <= 7
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(score) OVER (PARTITION BY YEAR(enrolled_at)) AS enrolled_at_year_max_score,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, student_id), 2) AS score_vs_grade_status_student_id_avg,
        ROW_NUMBER() OVER (PARTITION BY YEAR(enrolled_at), student_id ORDER BY enrolled_at DESC, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at)) AS rows_in_enrolled_at_year,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        MAX(score) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        ROUND(AVG(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_avg_2
    FROM cleaned_enrollments
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE seq_no <= 5
      AND score_avg_2 >= 15.0
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
        enrolled_at_year_max_score,
        seq_no,
        rows_in_enrolled_at_year,
        first_known_enrolled_at,
        max_score_so_far,
        score_avg_2,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY YEAR(first_known_enrolled_at), course_id), 2) AS seq_no_vs_first_known_enrolled_at_year_course_id,
        ROUND(rows_in_enrolled_at_year / NULLIF(SUM(rows_in_enrolled_at_year) OVER (PARTITION BY YEAR(first_known_enrolled_at), course_id), 0), 4) AS rows_in_enrolled_at_year_share_of_first_known_en,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (ORDER BY score_avg_2 ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        max_score_so_far - LAG(max_score_so_far) OVER w_all_by_score_avg_2 AS max_score_so_far_change
    FROM first_rows
    WINDOW w_all_by_score_avg_2 AS (ORDER BY score_avg_2 ASC, id)
)
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    enrolled_at_year_max_score,
    seq_no,
    rows_in_enrolled_at_year,
    first_known_enrolled_at,
    max_score_so_far,
    score_avg_2,
    seq_no_vs_first_known_enrolled_at_year_course_id,
    minutes_since_prev_enrolled_at,
    max_score_so_far_change,
    ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(first_known_enrolled_at)) / 60, 1) AS minutes_first_known_enrolled_at_to_enrolled_at,
    CONCAT(grade_status, '#', CAST(rows_in_enrolled_at_year AS STRING)) AS grade_status_ref
FROM metrics
ORDER BY score_avg_2 DESC, seq_no_vs_first_known_enrolled_at_year_course_id DESC

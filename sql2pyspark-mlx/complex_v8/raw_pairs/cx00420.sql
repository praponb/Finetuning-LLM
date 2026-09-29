WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 11:00:00'
      AND student_id >= 16
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN score >= 83.1 THEN 'large' WHEN score >= 11.3 THEN 'medium' ELSE 'small' END AS score_tier
    FROM prepared_enrollments
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(score_tier) OVER w_course_id_enrolled_at_month) AS distinct_score_tier_in_course_id_enrolled_at_mon,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_percent_rank,
        RANK() OVER (ORDER BY enrolled_at, id) AS enrolled_at_rank,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS seq_no,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior
    FROM labeled
    WINDOW w_course_id_enrolled_at_month AS (PARTITION BY course_id, MONTH(enrolled_at)), w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id)
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.enrolled_at < TIMESTAMP '2025-02-24 13:00:00'
              AND e.course_id >= 6
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        COALESCE(seq_no, 0) AS seq_no_filled,
        LENGTH(TRIM(score_tier)) AS score_tier_len
    FROM filtered
    WHERE enrolled_at_rank < 8
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_percent_rank,
        s.enrolled_at_rank,
        s.seq_no,
        s.enrolled_at_month,
        s.seq_no_filled,
        c.course_code,
        c.level AS course_level,
        c.dept AS course_dept
    FROM annotated s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept NOT IN ('CS', 'Math')
)
SELECT
    id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    enrolled_at_percent_rank,
    enrolled_at_rank,
    seq_no,
    course_level
FROM enriched_course
WHERE enrolled_at_percent_rank >= 0.1
ORDER BY score DESC

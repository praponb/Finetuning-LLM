WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.5, 2) AS score_adj,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
),
without_matches AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.credits >= 2
        )
),
grade_status_stats AS (
    -- Median score_adj per grade_status
    SELECT grade_status, PERCENTILE_APPROX(score_adj, 0.5) AS median_score_adj
    FROM without_matches
    GROUP BY grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score_adj,
        ROUND(s.score_adj - g.median_score_adj, 2) AS score_adj_minus_median
    FROM without_matches s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score_adj >= g.median_score_adj
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.major AS student_major,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on
    FROM compared s
    JOIN students st ON st.id = s.student_id
    WHERE st.is_international = TRUE
),
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        s.enrolled_at_label,
        s.median_score_adj,
        s.student_major,
        s.student_is_international,
        s.student_enrolled_on,
        st.cohort AS student_cohort
    FROM with_details s
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        enrolled_at,
        score_adj,
        enrolled_at_label,
        median_score_adj,
        student_major,
        student_is_international,
        student_enrolled_on,
        SIZE(COLLECT_SET(student_id) OVER w_student_major) AS distinct_student_id_in_student_major,
        SUM(score_adj) OVER (PARTITION BY student_major) AS student_major_sum_score_adj,
        ROUND(AVG(score_adj) OVER (ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_adj_avg_7,
        MIN(score_adj) OVER w_student_major AS student_major_min_score_adj
    FROM enriched_student
    WINDOW w_student_major AS (PARTITION BY student_major)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(student_major, 'UNKNOWN') AS student_major_or_unknown,
        LENGTH(TRIM(student_major)) AS student_major_len
    FROM sequenced
)
SELECT
    id,
    student_id,
    course_id,
    enrolled_at,
    score_adj,
    enrolled_at_label,
    median_score_adj,
    student_major,
    student_is_international,
    student_enrolled_on,
    distinct_student_id_in_student_major,
    student_major_sum_score_adj,
    score_adj_avg_7,
    student_major_min_score_adj,
    student_major_or_unknown,
    student_major_len
FROM labeled
ORDER BY student_major NULLS LAST, id
LIMIT 10

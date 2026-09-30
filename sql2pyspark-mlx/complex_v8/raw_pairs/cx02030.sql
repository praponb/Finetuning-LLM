WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM enrollments
    WHERE (course_id >= 3 OR enrolled_at < TIMESTAMP '2025-02-18 08:00:00')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (ORDER BY enrolled_at ASC) AS enrolled_at_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_percent_rank,
        LAST_VALUE(score) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score
    FROM scoped_enrollments
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at ASC)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(enrolled_at_percent_rank * 1.07, 2) AS enrolled_at_percent_rank_adj,
        enrolled_at_rank - score AS enrolled_at_rank_minus_score,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM windowed
    WHERE score > 20.2
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at_label,
        s.enrolled_at_rank,
        s.enrolled_at_percent_rank,
        s.enrolled_at_percent_rank_adj,
        s.enrolled_at_rank_minus_score,
        s.enrolled_at_year,
        c.course_code,
        st.cohort AS student_cohort
    FROM tagged s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        grade_status,
        enrolled_at_label,
        enrolled_at_rank,
        enrolled_at_percent_rank,
        enrolled_at_percent_rank_adj,
        enrolled_at_rank_minus_score,
        enrolled_at_year,
        course_code,
        student_cohort,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY score ASC, id) AS score_change
    FROM with_parent_course
)
SELECT
    id,
    grade_status,
    enrolled_at_label,
    enrolled_at_rank,
    enrolled_at_percent_rank_adj,
    enrolled_at_year,
    course_code,
    student_cohort,
    distinct_student_id_in_course_id,
    score_change
FROM sequenced
WHERE enrolled_at_rank >= 5
ORDER BY course_code DESC NULLS FIRST

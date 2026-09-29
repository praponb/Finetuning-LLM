WITH valid AS (
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
    WHERE student_id IS NOT NULL
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week,
        ROUND(score * 1.5, 2) AS score_adj,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM valid
    WHERE grade_status = 'PASS'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        enrolled_at_week,
        score_adj,
        enrolled_at_label,
        LOWER(REGEXP_REPLACE(TRIM(enrolled_at_label), ' ', '_')) AS enrolled_at_label_slug
    FROM annotated
    WHERE student_id <= 19
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at_week,
        s.score_adj,
        s.enrolled_at_label_slug,
        st.student_name,
        st.major AS student_major
    FROM tagged s
    JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    enrolled_at_week,
    score_adj,
    enrolled_at_label_slug,
    student_major,
    DATEDIFF(DATE '2026-01-01', enrolled_at_week) AS days_since_enrolled_at_week
FROM with_parent_student
WHERE student_id IS NOT NULL
ORDER BY enrolled_at_week DESC, score_adj DESC

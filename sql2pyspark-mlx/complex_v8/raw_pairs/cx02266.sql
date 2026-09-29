WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        YEAR(enrolled_at) AS enrolled_at_year,
        CONCAT_WS('-', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE course_id <= 8
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_year,
        grade_status_term_label,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC), 3) AS score_percent_rank,
        MAX(enrolled_at_year) OVER w_course_id AS course_id_max_enrolled_at_year,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id
    FROM valid
    WINDOW w_course_id AS (PARTITION BY course_id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN enrolled_at_year >= 2026 THEN 'A' WHEN enrolled_at_year >= 2025 THEN 'B' ELSE 'C' END AS enrolled_at_year_tier,
        CONCAT_WS('-', term, grade_status_term_label) AS term_grade_status_term_label_label,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix
    FROM sequenced
    WHERE term NOT IN ('2025T2', '2025T1')
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at_2
    FROM derived s
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year_2,
        CONCAT_WS('-', grade_status_term_label, enrolled_at_year_tier) AS grade_status_term_label_enrolled_at_year_tier_la
    FROM with_counts
    WHERE term_grade_status_term_label_label IS NOT NULL
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.enrolled_at,
        s.enrolled_at_year,
        s.grade_status_term_label,
        s.score_percent_rank,
        s.score_share_of_course_id,
        s.first_enrollment_enrolled_at_2,
        s.enrolled_at_year_2,
        s.grade_status_term_label_enrolled_at_year_tier_la,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score,
        e.course_id AS enrollment_course_id
    FROM labeled s
    JOIN enrollments e ON e.student_id = s.student_id AND e.score BETWEEN 5.6 AND 78.8
),
joined_enrollment AS (
    -- Join enrollments
    SELECT s.*, e.course_id AS enrollment_course_id_2, c.level AS course_level
    FROM with_children_enrollments s
    LEFT JOIN enrollments e ON e.id = s.enrollment_id
    LEFT JOIN courses c ON c.id = e.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    enrolled_at_year,
    grade_status_term_label,
    score_percent_rank,
    score_share_of_course_id,
    first_enrollment_enrolled_at_2,
    enrolled_at_year_2,
    grade_status_term_label_enrolled_at_year_tier_la,
    enrollment_id,
    enrollment_term,
    enrollment_course_id_2,
    course_level,
    CASE WHEN score_share_of_course_id >= 0.6 THEN 'HIGH' WHEN score_share_of_course_id >= 0.5 THEN 'MEDIUM' ELSE 'LOW' END AS score_share_of_course_id_tier,
    CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
FROM joined_enrollment
WHERE score_share_of_course_id >= 0.1
ORDER BY term_ref DESC NULLS FIRST

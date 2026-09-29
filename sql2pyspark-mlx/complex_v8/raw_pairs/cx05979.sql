WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        YEAR(enrolled_at) AS enrolled_at_year,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE course_id >= 3
      AND score <= 74.9
),
combined_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.grade_status_term_label,
        c.dept AS course_dept
    FROM cleaned_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.dept = 'CS'
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM combined_details_course s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        score,
        grade_status,
        course_dept,
        LENGTH(TRIM(grade_status_term_label)) AS grade_status_term_label_len,
        CONCAT(grade_status_term_label, '#', CAST(course_id AS STRING)) AS grade_status_term_label_ref,
        CASE WHEN score >= 69.6 THEN 'large' WHEN score >= 30.0 THEN 'medium' ELSE 'small' END AS score_tier
    FROM screened
    WHERE grade_status IN ('FAIL', 'WITHDRAWN')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(grade_status_term_label_len - AVG(grade_status_term_label_len) OVER (PARTITION BY score_tier, course_dept), 2) AS grade_status_term_label_len_vs_score_tier_course,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status_term_label_len ORDER BY score DESC), 3) AS score_cume_dist
    FROM derived
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE grade_status_term_label_len_vs_score_tier_course < (SELECT MAX(grade_status_term_label_len_vs_score_tier_course) FROM metrics)
)
SELECT
    id,
    score,
    course_dept,
    grade_status_term_label_ref,
    grade_status_term_label_len_vs_score_tier_course,
    ROUND(score_cume_dist / NULLIF(score, 0), 3) AS score_cume_dist_per_score
FROM without_matches
WHERE score_tier IN ('large', 'small')
ORDER BY grade_status_term_label_len_vs_score_tier_course DESC, grade_status_term_label_ref

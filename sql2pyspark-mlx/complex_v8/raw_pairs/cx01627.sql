WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        INITCAP(TRIM(term)) AS term_clean
    FROM enrollments
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM base_enrollments s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS')
              AND e.score > 11.1
        )
      AND s.term IN ('2024T1', '2024T2')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_id,
        term,
        score,
        MAX(score) OVER w_course_id AS course_id_max_score,
        ROUND(score / NULLIF(SUM(score) OVER w_course_id, 0), 4) AS score_share_of_course_id,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score
    FROM without_matches
    WINDOW w_course_id AS (PARTITION BY course_id)
),
course_id_stats AS (
    -- Median score_share_of_course_id per course_id
    SELECT course_id, PERCENTILE_APPROX(score_share_of_course_id, 0.5) AS median_score_share_of_course_id
    FROM with_trends
    GROUP BY course_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score_share_of_course_id,
        ROUND(s.score_share_of_course_id - g.median_score_share_of_course_id, 2) AS score_share_of_course_id_minus_median
    FROM with_trends s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score_share_of_course_id >= g.median_score_share_of_course_id
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status IN ('WITHDRAWN', 'FAIL', 'INCOMPLETE')
        )
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.median_score_share_of_course_id,
        s.score_share_of_course_id_minus_median,
        c.credits AS course_credits,
        c.level AS course_level,
        c.dept AS course_dept
    FROM qualified s
    LEFT JOIN courses c ON c.id = s.course_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_parent
    WHERE median_score_share_of_course_id > (SELECT AVG(median_score_share_of_course_id) FROM with_parent)
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    median_score_share_of_course_id,
    score_share_of_course_id_minus_median,
    course_credits,
    course_level,
    course_dept,
    COALESCE(course_credits, 0) AS course_credits_filled,
    CASE WHEN course_credits >= 3 THEN 'HIGH' WHEN course_credits >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS course_credits_tier
FROM screened
ORDER BY course_dept DESC NULLS FIRST, id
LIMIT 20

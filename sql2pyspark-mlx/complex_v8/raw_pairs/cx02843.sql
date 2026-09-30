WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown_2
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 09:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        days_since_enrolled_at,
        grade_status_or_unknown,
        ROUND(AVG(score) OVER (PARTITION BY YEAR(enrolled_at)), 2) AS enrolled_at_year_avg_score,
        LEAD(enrolled_at) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS next_enrolled_at,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status_or_unknown_2 ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at)), 0), 4) AS score_share_of_enrolled_at_year
    FROM recent
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.grade_status_or_unknown,
        s.enrolled_at_year_avg_score,
        s.next_enrolled_at,
        s.second_score,
        s.score_share_of_enrolled_at_year,
        c.course_code
    FROM with_trends s
    JOIN courses c ON c.id = s.course_id
),
enriched AS (
    -- Join courses
    SELECT s.*, c.dept AS course_dept, c.credits AS course_credits
    FROM joined s
    JOIN courses c ON c.id = s.course_id
),
combined_details AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.grade_status_or_unknown,
        s.enrolled_at_year_avg_score,
        s.next_enrolled_at,
        s.score_share_of_enrolled_at_year,
        s.course_code,
        c.dept AS course_dept_2
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept IN ('Art', 'CS', 'Math')
)
SELECT
    id,
    term,
    score,
    days_since_enrolled_at,
    grade_status_or_unknown,
    enrolled_at_year_avg_score,
    score_share_of_enrolled_at_year,
    course_code,
    DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at_2,
    LENGTH(TRIM(course_dept_2)) AS course_dept_2_len
FROM combined_details
ORDER BY days_since_enrolled_at NULLS LAST

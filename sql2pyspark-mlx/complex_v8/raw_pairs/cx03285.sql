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
        ABS(score - 38.5) AS score_distance,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE term = '2024T1'
      AND grade_status = 'INCOMPLETE'
),
joined_course AS (
    -- Attach course attributes
    SELECT s.*, c.course_code
    FROM recent s
    JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        score_distance,
        enrolled_at_dayofweek,
        course_code,
        enrolled_at_dayofweek - LAG(enrolled_at_dayofweek) OVER w_grade_status_by_enrolled_at AS enrolled_at_dayofweek_change,
        LAG(enrolled_at, 3) OVER w_all_by_enrolled_at AS prev3_enrolled_at,
        COUNT(*) OVER (PARTITION BY course_id, term) AS rows_in_course_id_term,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY term, enrolled_at_dayofweek)) AS distinct_grade_status_in_term_enrolled_at_dayofw,
        ROUND(enrolled_at_dayofweek - AVG(enrolled_at_dayofweek) OVER (PARTITION BY term, enrolled_at_dayofweek), 2) AS enrolled_at_dayofweek_vs_term_enrolled_at_dayofw,
        ROUND(score_distance / NULLIF(SUM(score_distance) OVER w_course_id_term, 0), 4) AS score_distance_share_of_course_id_term,
        COUNT(*) OVER (PARTITION BY term, enrolled_at_dayofweek) AS rows_in_term_enrolled_at_dayofweek,
        SUM(enrolled_at_dayofweek) OVER w_term_enrolled_at_dayofweek AS term_enrolled_at_dayofweek_sum_enrolled_at_dayof,
        ROUND(AVG(score_distance) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS score_distance_avg_2,
        ROUND(AVG(enrolled_at_dayofweek) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrolled_at_dayofweek_avg_2_prior
    FROM joined_course
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at DESC NULLS FIRST, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_course_id_term AS (PARTITION BY course_id, term), w_term_enrolled_at_dayofweek AS (PARTITION BY term, enrolled_at_dayofweek)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM with_windows
)
SELECT
    id,
    student_id,
    grade_status,
    score_distance,
    enrolled_at_dayofweek,
    course_code,
    enrolled_at_dayofweek_change,
    rows_in_course_id_term,
    distinct_grade_status_in_term_enrolled_at_dayofw,
    enrolled_at_dayofweek_vs_term_enrolled_at_dayofw,
    score_distance_share_of_course_id_term,
    rows_in_term_enrolled_at_dayofweek,
    term_enrolled_at_dayofweek_sum_enrolled_at_dayof,
    grade_status_slug,
    grade_status_or_unknown,
    term_prefix
FROM derived
WHERE enrolled_at >= TIMESTAMP '2025-01-25 13:00:00'
ORDER BY rows_in_course_id_term DESC, enrolled_at_dayofweek DESC

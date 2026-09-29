WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year,
        CASE WHEN score >= 73.0 THEN 'HIGH' WHEN score >= 42.4 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE term NOT IN ('2025T2', '2025T1')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(enrolled_at_year) OVER (PARTITION BY enrolled_at_year, grade_status), 2) AS enrolled_at_year_grade_status_avg_enrolled_at_ye,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY grade_status, term)) AS distinct_course_id_in_grade_status_term,
        DENSE_RANK() OVER (PARTITION BY course_id, enrolled_at_year ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        COUNT(*) OVER (PARTITION BY enrolled_at_year, grade_status) AS rows_in_enrolled_at_year_grade_status,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_id, enrolled_at_year ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROW_NUMBER() OVER w_score_tier_by_enrolled_at_ye AS enrolled_at_year_seq,
        enrolled_at_year - LAG(enrolled_at_year) OVER (PARTITION BY course_id, enrolled_at_year ORDER BY enrolled_at, id) AS enrolled_at_year_change,
        ROW_NUMBER() OVER (PARTITION BY course_id, enrolled_at_year ORDER BY enrolled_at, id) AS rn
    FROM recent
    WINDOW w_score_tier_by_enrolled_at_ye AS (PARTITION BY score_tier ORDER BY enrolled_at_year ASC NULLS LAST, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        score_tier,
        distinct_course_id_in_grade_status_term,
        rows_in_enrolled_at_year_grade_status,
        minutes_since_prev_enrolled_at,
        enrolled_at_year_seq,
        enrolled_at_year_change,
        rn,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY distinct_course_id_in_grade_status_term ORDER BY enrolled_at_year_grade_status_avg_enrolled_at_ye ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at_year, id), 3) AS enrolled_at_year_percent_rank,
        ROUND(enrolled_at_dense_rank / NULLIF(SUM(enrolled_at_dense_rank) OVER w_student_id, 0), 4) AS enrolled_at_dense_rank_share_of_student_id,
        LAG(enrolled_at_dense_rank) OVER (PARTITION BY distinct_course_id_in_grade_status_term ORDER BY enrolled_at_year_grade_status_avg_enrolled_at_ye ASC, id) AS prev_enrolled_at_dense_rank,
        SUM(enrolled_at_year) OVER w_student_id AS student_id_sum_enrolled_at_year,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_id)) AS distinct_grade_status_in_student_id,
        LEAD(enrolled_at_year, 3) OVER (PARTITION BY enrolled_at_dense_rank ORDER BY enrolled_at, id) AS next3_enrolled_at_year,
        LEAD(enrolled_at_dense_rank) OVER (ORDER BY enrolled_at_year, id) AS next_enrolled_at_dense_rank
    FROM sequenced
    WINDOW w_student_id AS (PARTITION BY student_id)
)
SELECT
    id,
    term,
    grade_status,
    enrolled_at,
    score_tier,
    distinct_course_id_in_grade_status_term,
    rows_in_enrolled_at_year_grade_status,
    minutes_since_prev_enrolled_at,
    enrolled_at_year_change,
    rn,
    last_known_enrolled_at,
    prev_enrolled_at_dense_rank,
    student_id_sum_enrolled_at_year,
    next3_enrolled_at_year,
    next_enrolled_at_dense_rank
FROM sequenced_2
ORDER BY enrolled_at DESC

WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LOWER(TRIM(term)) AS term_clean,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE term = '2025T2'
      AND course_id IS NOT NULL
),
enriched_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_clean,
        s.grade_status_group,
        st.cohort AS student_cohort
    FROM recent_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        term_clean,
        student_cohort,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        COUNT(*) OVER (PARTITION BY student_cohort) AS rows_in_student_cohort,
        SUM(score) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(CUME_DIST() OVER (PARTITION BY student_cohort ORDER BY score), 3) AS score_cume_dist,
        RANK() OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at) AS enrolled_at_rank,
        SIZE(COLLECT_SET(term) OVER w_student_cohort) AS distinct_term_in_student_cohort,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY score ASC, id) AS score_seq
    FROM enriched_student
    WINDOW w_student_cohort AS (PARTITION BY student_cohort)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        term_clean,
        first_known_score,
        rows_in_student_cohort,
        running_score,
        score_cume_dist,
        distinct_term_in_student_cohort,
        score_seq,
        running_score - score_cume_dist AS running_score_minus_score_cume_dist
    FROM sequenced
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term_clean,
    first_known_score,
    rows_in_student_cohort,
    running_score,
    score_cume_dist,
    distinct_term_in_student_cohort,
    score_seq,
    running_score_minus_score_cume_dist,
    ROUND(score_cume_dist / NULLIF(running_score, 0), 3) AS score_cume_dist_per_running_score,
    ROUND(score_cume_dist / NULLIF(first_known_score, 0), 3) AS score_cume_dist_per_first_known_score
FROM enriched
ORDER BY distinct_term_in_student_cohort NULLS LAST, running_score DESC

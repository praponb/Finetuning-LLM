WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CASE WHEN term IN ('2024T2', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status NOT IN ('PASS', 'INCOMPLETE')
      AND term = '2024T1'
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.term_group,
        s.grade_status_ref,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international
    FROM cleaned_enrollments s
    JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        term,
        term_group,
        grade_status_ref,
        student_major,
        student_enrolled_on,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        SIZE(COLLECT_SET(term_group) OVER w_course_id) AS distinct_term_group_in_course_id,
        LAST_VALUE(student_enrolled_on) OVER (PARTITION BY term_group, student_is_international ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_enrolled_on,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, term_group), 0), 4) AS score_share_of_student_id_term_group,
        ROUND(AVG(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        RANK() OVER (PARTITION BY term_group, student_is_international ORDER BY enrolled_at) AS enrolled_at_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id)) AS distinct_term_in_course_id,
        LAG(score) OVER (ORDER BY student_enrolled_on, id) AS prev_score,
        LAG(student_enrolled_on) OVER w_all_by_score AS prev_student_enrolled_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        RANK() OVER (ORDER BY score) AS score_rank,
        ROW_NUMBER() OVER (PARTITION BY term_group, student_is_international ORDER BY enrolled_at, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at, id) AS enrolled_at_seq
    FROM joined
    WINDOW w_course_id AS (PARTITION BY course_id), w_all_by_score AS (ORDER BY score, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        term_group,
        student_major,
        student_enrolled_on,
        score_share_of_course_id,
        score_share_of_student_id_term_group,
        score_avg_6_prior,
        rows_in_course_id,
        enrolled_at_rank,
        distinct_term_in_course_id,
        prev_student_enrolled_on,
        enrolled_at_percent_rank,
        rn,
        enrolled_at_seq,
        ROUND(rows_in_course_id / NULLIF(prev_score, 0), 3) AS rows_in_course_id_per_prev_score,
        YEAR(last_student_enrolled_on) AS last_student_enrolled_on_year
    FROM windowed
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term_group,
        student_major,
        student_enrolled_on,
        score_share_of_course_id,
        score_avg_6_prior,
        rows_in_course_id,
        enrolled_at_rank,
        distinct_term_in_course_id,
        prev_student_enrolled_on,
        rn,
        enrolled_at_seq,
        rows_in_course_id_per_prev_score,
        last_student_enrolled_on_year,
        SIZE(COLLECT_SET(term_group) OVER w_last_student_enrolled_on_yea) AS distinct_term_group_in_last_student_enrolled_on_,
        SUM(enrolled_at_seq) OVER (ORDER BY score_share_of_student_id_term_group ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_term_in_course_id, rn ORDER BY last_student_enrolled_on_year DESC), 3) AS last_student_enrolled_on_year_cume_dist
    FROM tagged
    WINDOW w_last_student_enrolled_on_yea AS (PARTITION BY last_student_enrolled_on_year)
)
SELECT DISTINCT enrolled_at_seq
FROM ranked
WHERE enrolled_at_rank BETWEEN 2 AND 7
ORDER BY enrolled_at_seq NULLS LAST

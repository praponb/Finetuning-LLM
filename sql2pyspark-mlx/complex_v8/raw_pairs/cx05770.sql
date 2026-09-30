WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE score <= 62.5
),
joined AS (
    -- Attach course attributes
    SELECT s.*, c.level AS course_level, st.cohort AS student_cohort
    FROM cleaned_enrollments s
    JOIN courses c ON c.id = s.course_id AND UPPER(TRIM(c.course_code)) LIKE 'C%'
    LEFT JOIN students st ON st.id = s.student_id
    WHERE c.level <> 'core'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(course_level) OVER (PARTITION BY grade_status, student_id)) AS distinct_course_level_in_grade_status_student_id,
        ROUND(PERCENT_RANK() OVER w_student_cohort_by_enrolled_a, 3) AS enrolled_at_percent_rank,
        ROUND(score - AVG(score) OVER w_grade_status_student_id, 2) AS score_vs_grade_status_student_id_avg,
        MIN(score) OVER w_grade_status_student_id AS grade_status_student_id_min_score,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY student_cohort ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        LEAD(enrolled_at, 3) OVER (PARTITION BY student_cohort ORDER BY enrolled_at NULLS LAST, id) AS next3_enrolled_at,
        ROUND(score_adj - AVG(score_adj) OVER (PARTITION BY grade_status, student_id), 2) AS score_adj_vs_grade_status_student_id_avg,
        SIZE(COLLECT_SET(grade_status_group) OVER (PARTITION BY grade_status, term)) AS distinct_grade_status_group_in_grade_status_term,
        LEAD(enrolled_at, 2) OVER (PARTITION BY student_cohort, TO_DATE(enrolled_at) ORDER BY score_adj, id) AS next2_enrolled_at
    FROM joined
    WINDOW w_student_cohort_by_enrolled_a AS (PARTITION BY student_cohort ORDER BY enrolled_at NULLS LAST), w_grade_status_student_id AS (PARTITION BY grade_status, student_id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        LOWER(REGEXP_REPLACE(TRIM(student_cohort), ' ', '_')) AS student_cohort_slug
    FROM ranked
    WHERE course_level IS NOT NULL
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        score,
        enrolled_at,
        score_adj,
        grade_status_group,
        course_level,
        student_cohort,
        distinct_course_level_in_grade_status_student_id,
        enrolled_at_percent_rank,
        score_vs_grade_status_student_id_avg,
        grade_status_student_id_min_score,
        score_adj_vs_grade_status_student_id_avg,
        next2_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_grade_status_group_student_i, 3) AS score_adj_percent_rank,
        ROW_NUMBER() OVER w_grade_status_group_student_i_2 AS seq_no,
        NTH_VALUE(enrolled_at_percent_rank, 2) OVER (PARTITION BY distinct_grade_status_group_in_grade_status_term ORDER BY score_adj_vs_grade_status_student_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_percent_rank
    FROM scored
    WINDOW w_grade_status_group_student_i AS (PARTITION BY grade_status_group, student_id ORDER BY score_adj ASC), w_grade_status_group_student_i_2 AS (PARTITION BY grade_status_group, student_id ORDER BY score_adj ASC, id)
)
SELECT
    id,
    student_id,
    term,
    score,
    grade_status_group,
    course_level,
    student_cohort,
    distinct_course_level_in_grade_status_student_id,
    score_vs_grade_status_student_id_avg,
    grade_status_student_id_min_score,
    score_adj_vs_grade_status_student_id_avg,
    next2_enrolled_at,
    score_adj_percent_rank,
    LEAST(score_adj_percent_rank, seq_no) AS least_score_adj_percent_rank_seq_no,
    LOWER(REGEXP_REPLACE(TRIM(grade_status_group), ' ', '_')) AS grade_status_group_slug
FROM ranked_2
ORDER BY distinct_course_level_in_grade_status_student_id DESC

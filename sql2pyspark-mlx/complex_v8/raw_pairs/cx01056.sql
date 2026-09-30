WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        c.dept AS course_dept,
        c.course_code,
        st.is_international AS student_is_international,
        st.gpa AS student_gpa
    FROM cleaned s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        course_dept,
        course_code,
        student_gpa,
        CONCAT(course_dept, '#', CAST(course_id AS STRING)) AS course_dept_ref
    FROM enriched
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        score,
        course_code,
        student_gpa,
        course_dept_ref,
        RANK() OVER w_all_by_score_ties AS score_rank,
        ROUND(score - AVG(score) OVER w_term, 2) AS score_vs_term_avg,
        MAX(student_gpa) OVER (PARTITION BY course_dept, student_id ORDER BY student_gpa, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS student_gpa_max_3_prior
    FROM scored
    WINDOW w_all_by_score_ties AS (ORDER BY score), w_term AS (PARTITION BY term)
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
              AND HOUR(e.enrolled_at) BETWEEN 10 AND 12
        )
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
        )
      AND s.term NOT IN ('2025T1', '2024T1')
)
SELECT
    id,
    student_id,
    term,
    score,
    student_gpa,
    course_dept_ref,
    score_rank,
    LOWER(REGEXP_REPLACE(TRIM(course_dept_ref), ' ', '_')) AS course_dept_ref_slug,
    ABS(score - 59.7) AS score_distance
FROM with_activity
ORDER BY course_dept_ref_slug, student_gpa NULLS LAST

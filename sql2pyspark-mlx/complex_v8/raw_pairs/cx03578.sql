WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 58.8 THEN 'A' WHEN score >= 13.9 THEN 'B' ELSE 'C' END AS score_tier,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND score > 32.7
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(score_tier, '#', CAST(course_id AS STRING)) AS score_tier_ref,
        CASE WHEN score_tier IN ('A', 'C') THEN 'group_a' WHEN score_tier = 'B' THEN 'group_b' ELSE 'other' END AS score_tier_group,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM scoped
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id >= 19
        )
),
grouped AS (
    -- Summarise per group
    SELECT
        course_id,
        student_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        ROUND(100 * COUNT(CASE WHEN grade_status <> 'WITHDRAWN' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_withdrawn,
        SUM(score) FILTER (WHERE term NOT IN ('2025T1', '2024T2')) AS score_term_not_in_2025t1_2024t2
    FROM filtered
    GROUP BY 1, 2
    HAVING AVG(score) > 20.6
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.course_id,
        s.student_id,
        s.distinct_course_ids,
        s.pct_grade_status_withdrawn,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international,
        st.enrolled_on AS student_enrolled_on,
        c.credits AS course_credits
    FROM grouped s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_distinct_course_ids AS rows_in_distinct_course_ids,
        ROW_NUMBER() OVER w_all_by_distinct_course_ids AS rn,
        course_credits - LAG(course_credits) OVER (ORDER BY distinct_course_ids, course_id, student_id) AS course_credits_change,
        SUM(student_gpa) OVER w_distinct_course_ids AS distinct_course_ids_sum_student_gpa,
        LEAD(distinct_course_ids) OVER w_course_id_by_distinct_course AS next_distinct_course_ids
    FROM with_parent_student
    WINDOW w_distinct_course_ids AS (PARTITION BY distinct_course_ids), w_all_by_distinct_course_ids AS (ORDER BY distinct_course_ids, course_id, student_id), w_course_id_by_distinct_course AS (PARTITION BY course_id ORDER BY distinct_course_ids NULLS LAST, course_id, student_id)
)
SELECT course_id, student_id, metric_name, reading
FROM (
    SELECT course_id, student_id, course_credits, rows_in_distinct_course_ids, rn
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric_name IN (course_credits AS m_course_credits, rows_in_distinct_course_ids AS rows_in_distinct_course_ids_v, rn AS m_rn)
) unp

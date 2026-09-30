WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE (enrolled_at < TIMESTAMP '2025-02-24 09:00:00' OR term IS NOT NULL)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        CASE WHEN term IN ('2025T2', '2024T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM valid_enrollments
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND MONTH(e.enrolled_at) IN (1, 2)
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        term_group,
        CONCAT_WS('|', term, term_group) AS term_term_group_label
    FROM filtered
    WHERE enrolled_at_label IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        term_group,
        term_term_group_label,
        CASE WHEN term IN ('2025T1', '2024T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group_2
    FROM enriched
    WHERE score < 63.9
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY term_group, course_id) AS rows_in_term_group_course_id,
        ROUND(AVG(score) OVER w_term_group_course_id, 2) AS term_group_course_id_avg_score,
        MIN(score) OVER (PARTITION BY term_group, course_id) AS term_group_course_id_min_score,
        score - LAG(score) OVER (PARTITION BY term ORDER BY score ASC, id) AS score_change,
        ROUND(score / NULLIF(SUM(score) OVER w_term_group_course_id, 0), 4) AS score_share_of_term_group_course_id
    FROM tagged
    WINDOW w_term_group_course_id AS (PARTITION BY term_group, course_id)
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    term_group,
    term_term_group_label,
    rows_in_term_group_course_id,
    term_group_course_id_avg_score,
    term_group_course_id_min_score,
    score_share_of_term_group_course_id
FROM with_trends
WHERE score BETWEEN 20.6 AND 92.4
ORDER BY student_id

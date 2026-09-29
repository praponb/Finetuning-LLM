WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE term = '2025T1'
      AND MONTH(enrolled_at) IN (1, 2)
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
        )
      AND s.term = '2024T1'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        term,
        score,
        CASE WHEN score >= 58.8 THEN 'large' WHEN score >= 35.8 THEN 'medium' ELSE 'small' END AS score_tier
    FROM with_activity
    WHERE student_id IS NOT NULL
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        term,
        score,
        score_tier,
        CASE WHEN term IN ('2024T2', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enriched
)
SELECT
    score_tier,
    term_group,
    term,
    SUM(CASE WHEN term <> '2024T2' THEN 1 ELSE 0 END) AS n_term_2024t2,
    ROUND(STDDEV(score), 2) AS stddev_score
FROM annotated
GROUP BY CUBE(score_tier, term_group, term)

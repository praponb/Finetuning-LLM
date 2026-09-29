WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
    WHERE student_id >= 2
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.term_or_unknown,
        s.score_adj,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status
    FROM filtered_enrollments s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
derived AS (
    -- Label and score rows
    SELECT *, ROUND(enrollment_score * 0.25, 2) AS enrollment_score_adj
    FROM line_level
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN enrollment_score >= 69.6 THEN 'A' WHEN enrollment_score >= 26.2 THEN 'B' ELSE 'C' END AS enrollment_score_tier,
        CONCAT(enrollment_grade_status, '#', CAST(student_id AS STRING)) AS enrollment_grade_status_ref
    FROM derived
    WHERE course_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        score,
        term_or_unknown,
        score_adj,
        enrollment_id,
        enrollment_score,
        enrollment_score_adj,
        enrollment_score_tier,
        enrollment_grade_status_ref,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrollment_id ORDER BY score_adj NULLS LAST, id, enrollment_id), 3) AS score_adj_percent_rank,
        NTH_VALUE(score_adj, 2) OVER (PARTITION BY enrollment_id ORDER BY score_adj NULLS LAST, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_adj
    FROM enriched
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        term_or_unknown,
        ROUND(STDDEV(score_adj), 2) AS stddev_score_adj,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT term_or_unknown) AS distinct_term_or_unknowns,
        ROUND(SUM(second_score_adj), 2) AS total_second_score_adj
    FROM windowed
    GROUP BY term_or_unknown
)
SELECT term_or_unknown, total_second_score_adj
FROM grouped
ORDER BY total_second_score_adj DESC NULLS FIRST, term_or_unknown DESC

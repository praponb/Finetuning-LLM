WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-07 10:00:00'
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS max_enrollment_score
    FROM scoped_enrollments s
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM summarized_rows
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status_ref,
        enrolled_at_label,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label_2,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label_3
    FROM scored
    WHERE term IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CONCAT(grade_status_ref, '#', CAST(student_id AS STRING)) AS grade_status_ref_ref,
        ROUND(score * 0.9, 2) AS score_adj
    FROM scored_2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(term) OVER w_term_or_unknown) AS distinct_term_in_term_or_unknown,
        MIN(score_adj) OVER (PARTITION BY term_or_unknown) AS term_or_unknown_min_score_adj,
        DENSE_RANK() OVER (ORDER BY score_adj) AS score_adj_dense_rank
    FROM derived
    WINDOW w_term_or_unknown AS (PARTITION BY term_or_unknown)
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(enrolled_at_label_2, '#', CAST(id AS STRING)) AS enrolled_at_label_2_ref,
        LOWER(REGEXP_REPLACE(TRIM(enrolled_at_label), ' ', '_')) AS enrolled_at_label_slug,
        ROUND(score * 0.9, 2) AS score_adj_2
    FROM sequenced
    WHERE enrolled_at_label_2 IS NOT NULL
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status_ref,
    enrolled_at_label_2,
    enrolled_at_label_3,
    term_or_unknown,
    grade_status_ref_ref,
    term_or_unknown_min_score_adj,
    score_adj_dense_rank,
    enrolled_at_label_slug,
    score_adj_2,
    ROUND(score_adj_2 * 0.25, 2) AS score_adj_2_adj
FROM derived_2
ORDER BY id, term_or_unknown NULLS LAST

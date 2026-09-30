WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
    FROM enrollments
    WHERE (course_id <= 10 OR student_id IS NOT NULL)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        term,
        ROUND(SUM(score), 2) AS total_score,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MIN(score) AS min_score,
        ROUND(STDDEV(score), 2) AS stddev_score,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM prepared_enrollments
    GROUP BY term
    HAVING SUM(score) > 32.7
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY term, distinct_student_ids ORDER BY distinct_grade_status DESC) AS distinct_grade_status_dense_rank,
        COUNT(*) OVER w_term_distinct_grade_status AS rows_in_term_distinct_grade_status
    FROM rollup_base
    WINDOW w_term_distinct_grade_status AS (PARTITION BY term, distinct_grade_status)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE distinct_grade_status < (SELECT MAX(distinct_grade_status) FROM with_windows)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(term)) AS term_len,
        distinct_student_ids + total_score AS distinct_student_ids_plus_total_score,
        LEAST(stddev_score, total_score) AS least_stddev_score_total_score
    FROM qualified
),
tagged AS (
    -- Derive helper columns
    SELECT
        term,
        total_score,
        min_score,
        stddev_score,
        distinct_grade_status_dense_rank,
        rows_in_term_distinct_grade_status,
        term_len,
        distinct_student_ids_plus_total_score,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        COALESCE(total_score, 0.0) AS total_score_filled,
        ROUND(total_score * 0.25, 2) AS total_score_adj
    FROM scored
)
SELECT
    term,
    min_score,
    distinct_grade_status_dense_rank,
    rows_in_term_distinct_grade_status,
    term_len,
    distinct_student_ids_plus_total_score,
    term_or_unknown,
    total_score_filled,
    total_score_adj,
    FLOOR(total_score_filled / 100) * 100 AS total_score_filled_bucket,
    LOWER(REGEXP_REPLACE(TRIM(term_or_unknown), ' ', '_')) AS term_or_unknown_slug
FROM tagged
ORDER BY rows_in_term_distinct_grade_status DESC, term_or_unknown DESC NULLS FIRST

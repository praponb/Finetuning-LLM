WITH base_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE student_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(enrolled_at_year / NULLIF(score, 0), 3) AS enrolled_at_year_per_score,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM base_enrollments
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(term_or_unknown)) AS term_or_unknown_len
    FROM enriched
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_student_id_enrolled_at_year AS term_or_unknown_len_dense_rank,
        ROUND(PERCENT_RANK() OVER w_student_id_enrolled_at_year_2, 3) AS term_or_unknown_len_percent_rank
    FROM tagged
    WINDOW w_student_id_enrolled_at_year AS (PARTITION BY student_id, enrolled_at_year ORDER BY term_or_unknown_len DESC), w_student_id_enrolled_at_year_2 AS (PARTITION BY student_id, enrolled_at_year ORDER BY term_or_unknown_len DESC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        term_or_unknown_len_dense_rank,
        MAX(enrolled_at) AS last_enrolled_at,
        ROUND(SUM(enrolled_at_year) / NULLIF(SUM(term_or_unknown_len_dense_rank), 0), 3) AS enrolled_at_year_to_term_or_unknown_len_dense_ra,
        MAX(enrolled_at_year_per_score) AS max_enrolled_at_year_per_score,
        ROUND(SUM(term_or_unknown_len) / NULLIF(SUM(term_or_unknown_len_dense_rank), 0), 3) AS term_or_unknown_len_to_term_or_unknown_len_dense,
        SUM(CASE WHEN grade_status IS NOT NULL THEN term_or_unknown_len_percent_rank ELSE 0 END) AS term_or_unknown_len_percent_rank_grade_status_is,
        SUM(term_or_unknown_len_percent_rank) FILTER (WHERE grade_status = 'INCOMPLETE') AS term_or_unknown_len_percent_rank_grade_status_in
    FROM sequenced
    GROUP BY term_or_unknown_len_dense_rank
)
-- Native SQL UNPIVOT (wide to long)
SELECT term_or_unknown_len_dense_rank, metric_name, reading
FROM (
    SELECT term_or_unknown_len_dense_rank, max_enrolled_at_year_per_score, term_or_unknown_len_to_term_or_unknown_len_dense, enrolled_at_year_to_term_or_unknown_len_dense_ra, term_or_unknown_len_percent_rank_grade_status_is
    FROM rollup_base
) src
UNPIVOT (
    reading FOR metric_name IN (max_enrolled_at_year_per_score, term_or_unknown_len_to_term_or_unknown_len_dense, enrolled_at_year_to_term_or_unknown_len_dense_ra, term_or_unknown_len_percent_rank_grade_status_is)
) unp

WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2024T2'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        NTILE(10) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC, id) AS enrolled_at_decile,
        ROW_NUMBER() OVER (ORDER BY enrolled_at, id) AS seq_no,
        LEAD(enrolled_at) OVER w_grade_status_by_enrolled_at AS next_enrolled_at,
        NTILE(5) OVER w_course_id_grade_status_by_en AS enrolled_at_quintile,
        DENSE_RANK() OVER (PARTITION BY course_id ORDER BY enrolled_at ASC) AS enrolled_at_dense_rank,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at
    FROM cleaned_enrollments
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at ASC, id), w_course_id_grade_status_by_en AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id)
),
summary AS (
    -- Group-level totals
    SELECT
        seq_no,
        course_id,
        YEAR(second_enrolled_at) AS second_enrolled_at_year,
        COUNT(*) FILTER (WHERE term <> '2025T2') AS n_term_2025t2,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        COUNT(*) AS cnt
    FROM sequenced
    GROUP BY seq_no, course_id, YEAR(second_enrolled_at)
)
-- Final projection
SELECT
    seq_no,
    second_enrolled_at_year,
    n_term_2025t2,
    n_term_is_not_null,
    distinct_grade_status,
    cnt
FROM summary
ORDER BY seq_no DESC

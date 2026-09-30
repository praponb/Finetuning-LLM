WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        DATE_TRUNC('QUARTER', enrolled_on) AS enrolled_on_quarter
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'H%'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(cohort)) AS cohort_len,
        INITCAP(TRIM(cohort)) AS cohort_clean,
        CONCAT(major, '#', CAST(id AS STRING)) AS major_ref
    FROM valid
    WHERE enrolled_on_quarter >= TIMESTAMP '2025-01-13 09:00:00'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        major,
        cohort_len,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown,
        CASE WHEN cohort_len >= 12 THEN 'A' WHEN cohort_len >= 6 THEN 'B' ELSE 'C' END AS cohort_len_tier
    FROM labeled
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        major_or_unknown,
        cohort_len_tier,
        SUM(cohort_len) OVER (PARTITION BY cohort_len_tier) AS cohort_len_tier_sum_cohort_len,
        MIN(cohort_len) OVER w_cohort_len_tier AS cohort_len_tier_min_cohort_len
    FROM derived
    WINDOW w_cohort_len_tier AS (PARTITION BY cohort_len_tier)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY major_or_unknown, cohort_len_tier ORDER BY cohort_len_tier_sum_cohort_len, id), 3) AS cohort_len_tier_sum_cohort_len_cume_dist,
        RANK() OVER w_major_or_unknown_cohort_len AS cohort_len_tier_sum_cohort_len_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY major_or_unknown, cohort_len_tier ORDER BY cohort_len_tier_sum_cohort_len, id), 3) AS cohort_len_tier_sum_cohort_len_percent_rank
    FROM sequenced
    WINDOW w_major_or_unknown_cohort_len AS (PARTITION BY major_or_unknown, cohort_len_tier ORDER BY cohort_len_tier_sum_cohort_len, id)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MIN(enrolled_at) AS first_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at
    FROM with_windows s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.cohort_len_tier_min_cohort_len,
        s.cohort_len_tier_sum_cohort_len_cume_dist,
        s.cohort_len_tier_sum_cohort_len_rank,
        s.cohort_len_tier_sum_cohort_len_percent_rank,
        e.id AS enrollment_id,
        e.course_id AS enrollment_course_id,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_stats s
    LEFT JOIN enrollments e ON e.student_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    enrollment_course_id,
    enrollment_grade_status,
    SUM(CASE WHEN enrollment_grade_status = 'FAIL' THEN 1 ELSE 0 END) AS n_enrollment_grade_status_fail,
    MIN(cohort_len_tier_sum_cohort_len_rank) AS min_cohort_len_tier_sum_cohort_len_rank,
    COUNT(DISTINCT enrollment_grade_status) AS distinct_enrollment_grade_status
FROM with_children
GROUP BY ROLLUP(enrollment_course_id, enrollment_grade_status)

WITH filtered_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa
    FROM students
    WHERE (gpa < 3.4 OR major NOT IN ('Art', 'Bio'))
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_name,
        major,
        enrolled_on,
        gpa,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown,
        ADD_MONTHS(enrolled_on, 12) AS enrolled_on_plus_12m,
        LOWER(REGEXP_REPLACE(TRIM(student_name), ' ', '_')) AS student_name_slug
    FROM filtered_students
    WHERE major <> 'CS'
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at_2,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.id) AS max_enrollment_score
    FROM enriched s
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(enrolled_on_plus_12m, 'yyyy-MM') AS enrolled_on_plus_12m_label
    FROM with_stats
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 08:00:00'
        )
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', first_enrollment_enrolled_at_2) AS first_enrollment_enrolled_at_2_month,
        COUNT(*) AS cnt,
        ROUND(SUM(gpa), 2) AS total_gpa,
        MAX(first_enrollment_enrolled_at_2) AS last_first_enrollment_enrolled_at_2,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(gpa), 2) AS avg_gpa,
        SUM(CASE WHEN major <> 'Bio' THEN 1 ELSE 0 END) AS n_major_bio
    FROM screened
    WHERE gpa <= 3.9
    GROUP BY DATE_TRUNC('MONTH', first_enrollment_enrolled_at_2)
),
annotated AS (
    -- Add calculated columns
    SELECT
        first_enrollment_enrolled_at_2_month,
        last_first_enrollment_enrolled_at_2,
        distinct_ids,
        avg_gpa,
        n_major_bio,
        LEAST(avg_gpa, cnt) AS least_avg_gpa_cnt,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket
    FROM totals
)
-- Final projection
SELECT
    first_enrollment_enrolled_at_2_month,
    last_first_enrollment_enrolled_at_2,
    distinct_ids,
    avg_gpa,
    n_major_bio,
    distinct_ids_bucket
FROM annotated
WHERE distinct_ids_bucket < 7
ORDER BY last_first_enrollment_enrolled_at_2 DESC, first_enrollment_enrolled_at_2_month
LIMIT 5

WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        enrolled_at + INTERVAL 2 HOURS AS enrolled_at_plus_2h,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE course_id IS NOT NULL
),
screened AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.credits BETWEEN 2 AND 4
              AND c.dept NOT IN ('Biz', 'Math')
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        grade_status,
        enrolled_at,
        term_slug,
        enrolled_at_plus_2h,
        enrolled_at_quarter,
        RANK() OVER w_student_id_enrolled_at_plus AS enrolled_at_rank
    FROM screened
    WINDOW w_student_id_enrolled_at_plus AS (PARTITION BY student_id, MONTH(enrolled_at_plus_2h) ORDER BY enrolled_at)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(term_slug)) AS term_slug_len,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM metrics
    WHERE enrolled_at_rank >= 4
),
latest AS (
    -- Keep the earliest row per grade_status_group, grade_status
    SELECT
        id,
        grade_status,
        enrolled_at,
        term_slug,
        enrolled_at_plus_2h,
        enrolled_at_quarter,
        enrolled_at_rank,
        term_slug_len,
        grade_status_group,
        grade_status_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status_group, grade_status ORDER BY enrolled_at_rank, id) AS recency_rank
        FROM derived
    ) ranked
    WHERE recency_rank = 1
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM latest
    WHERE enrolled_at_rank > (SELECT MIN(enrolled_at_rank) FROM latest)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(enrolled_at_plus_2h) - UNIX_TIMESTAMP(enrolled_at)) / 60, 1) AS minutes_enrolled_at_to_enrolled_at_plus_2h,
        FLOOR(enrolled_at_rank / 2) * 2 AS enrolled_at_rank_bucket,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label
    FROM filtered_2
)
-- Final projection
SELECT
    id,
    grade_status,
    enrolled_at,
    term_slug,
    enrolled_at_quarter,
    term_slug_len,
    grade_status_group,
    minutes_enrolled_at_to_enrolled_at_plus_2h,
    enrolled_at_rank_bucket,
    enrolled_at_label
FROM scored
ORDER BY grade_status_group DESC NULLS FIRST

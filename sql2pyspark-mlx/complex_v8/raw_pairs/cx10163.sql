WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('FAIL', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        FLOOR(score / 25) * 25 AS score_bucket,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'PASS', 'INCOMPLETE')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_bucket,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM cleaned
    WHERE score <= 66.0
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(score_bucket - AVG(score_bucket) OVER (PARTITION BY term, student_id), 2) AS score_bucket_vs_term_student_id_avg,
        ROW_NUMBER() OVER (PARTITION BY grade_status, student_id ORDER BY score_bucket ASC, id) AS score_bucket_seq,
        LEAD(enrolled_at) OVER (PARTITION BY grade_status, student_id ORDER BY score_bucket ASC, id) AS next_enrolled_at
    FROM scored
),
enriched AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(term)) AS term_len
    FROM ranked
)
SELECT
    id,
    student_id,
    term,
    score,
    grade_status,
    term_slug,
    score_bucket_vs_term_student_id_avg,
    next_enrolled_at,
    term_len,
    term_len + score_bucket AS term_len_plus_score_bucket,
    CASE WHEN term IN ('2024T1', '2024T2') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group
FROM enriched
ORDER BY term, term_slug

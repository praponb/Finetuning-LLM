WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        CASE WHEN score >= 63.9 THEN 'A' WHEN score >= 34.2 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE course_id >= 3
      AND student_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(score, 2) OVER w_score_tier_by_enrolled_at AS next2_score,
        DENSE_RANK() OVER (ORDER BY score_filled DESC) AS score_filled_dense_rank,
        COUNT(*) OVER w_grade_status AS rows_in_grade_status
    FROM recent
    WINDOW w_score_tier_by_enrolled_at AS (PARTITION BY score_tier ORDER BY enrolled_at DESC, id), w_grade_status AS (PARTITION BY grade_status)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        rows_in_grade_status + next2_score AS rows_in_grade_status_plus_next2_score,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        FLOOR(score_filled_dense_rank / 2) * 2 AS score_filled_dense_rank_bucket
    FROM sequenced
    WHERE grade_status IN ('WITHDRAWN', 'FAIL')
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id IS NOT NULL
        )
      AND s.enrolled_at < TIMESTAMP '2025-02-24 14:00:00'
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        score_tier,
        next2_score,
        rows_in_grade_status,
        rows_in_grade_status_plus_next2_score,
        days_since_enrolled_at,
        score_filled_dense_rank_bucket
    FROM with_activity
    WHERE score_filled_dense_rank <= 5
),
joined_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_filled,
        s.next2_score,
        s.days_since_enrolled_at,
        s.score_filled_dense_rank_bucket,
        c.level AS course_level
    FROM first_rows s
    LEFT JOIN courses c ON c.id = s.course_id
),
latest_per_key AS (
    -- Keep the earliest row per grade_status
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        next2_score,
        days_since_enrolled_at,
        score_filled_dense_rank_bucket,
        course_level
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at, id) AS row_num
        FROM joined_course
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    student_id,
    course_id,
    score_filled,
    next2_score,
    days_since_enrolled_at,
    score_filled_dense_rank_bucket,
    course_level,
    LOWER(TRIM(course_level)) AS course_level_clean,
    DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label
FROM latest_per_key
WHERE days_since_enrolled_at < 560
ORDER BY course_level_clean

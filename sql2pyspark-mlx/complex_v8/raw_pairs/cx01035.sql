WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix
    FROM enrollments
    WHERE course_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_filled,
        grade_status_prefix,
        ROUND(score_filled / NULLIF(SUM(score_filled) OVER (PARTITION BY MONTH(enrolled_at), course_id), 0), 4) AS score_filled_share_of_enrolled_at_month_course_i,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, course_id ORDER BY score DESC), 3) AS score_cume_dist
    FROM prepared_enrollments
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score >= 35.8
        )
      AND s.grade_status_prefix IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per grade_status, term
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_filled,
        grade_status_prefix,
        score_filled_share_of_enrolled_at_month_course_i,
        score_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, term ORDER BY score_cume_dist DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
)
SELECT id, measure, amount
FROM (
    SELECT id, score_cume_dist, score_filled
    FROM latest_per_key
) src
UNPIVOT (
    amount FOR measure IN (score_cume_dist, score_filled)
) unp

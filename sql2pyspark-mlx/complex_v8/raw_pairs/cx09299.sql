WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        ABS(score - 57.8) AS score_distance
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND term IN ('2025T1', '2025T2')
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status IS NOT NULL
        )
),
student_id_stats AS (
    -- Max score_distance per student_id
    SELECT student_id, MAX(score_distance) AS max_score_distance
    FROM filtered
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_score_distance,
        ROUND(s.score_distance - g.max_score_distance, 2) AS score_distance_minus_max
    FROM filtered s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score_distance = g.max_score_distance
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(score_distance - 35.0) AS score_distance_distance,
        max_score_distance - score_distance AS max_score_distance_minus_score_distance
    FROM compared
    WHERE score_distance > 30.0
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term NOT IN ('2025T1', '2025T2')
        )
      AND s.student_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    grade_status_prefix,
    grade_status_or_unknown,
    max_score_distance,
    score_distance_minus_max,
    ROUND(max_score_distance * 0.9, 2) AS max_score_distance_adj,
    GREATEST(score_distance_minus_max, score_distance_distance) AS greatest_score_distance_minus_max_score_distance
FROM screened
ORDER BY greatest_score_distance_minus_max_score_distance NULLS LAST

WITH base_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE term <> '2024T2'
      AND grade_status = 'WITHDRAWN'
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 4
        )
),
course_id_stats AS (
    -- Avg score per course_id
    SELECT course_id, AVG(score) AS avg_score
    FROM without_matches
    GROUP BY course_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM without_matches s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score > g.avg_score
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        grade_status_ref,
        enrolled_at_hour,
        enrolled_at_year,
        avg_score,
        score_minus_avg,
        CASE WHEN avg_score >= 65.0 THEN 'HIGH' WHEN avg_score >= 25.0 THEN 'MEDIUM' ELSE 'LOW' END AS avg_score_tier
    FROM above_average
    WHERE grade_status IN ('PASS', 'WITHDRAWN', 'FAIL')
),
latest AS (
    -- Keep the latest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        grade_status_ref,
        enrolled_at_hour,
        enrolled_at_year,
        avg_score,
        score_minus_avg,
        avg_score_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score DESC, id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_ref,
        enrolled_at_year,
        score_minus_avg,
        avg_score_tier,
        MIN(avg_score) OVER (PARTITION BY student_id, course_id) AS student_id_course_id_min_avg_score,
        COUNT(*) OVER (PARTITION BY student_id, course_id) AS rows_in_student_id_course_id,
        ROUND(avg_score / NULLIF(SUM(avg_score) OVER w_student_id_course_id, 0), 4) AS avg_score_share_of_student_id_course_id
    FROM latest
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id ORDER BY enrolled_at_year ASC NULLS LAST, id), 3) AS enrolled_at_year_cume_dist,
        NTH_VALUE(rows_in_student_id_course_id, 2) OVER (PARTITION BY student_id, avg_score_tier ORDER BY avg_score_share_of_student_id_course_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_student_id_course_id,
        ROUND(score / NULLIF(SUM(score) OVER w_enrolled_at_year_month, 0), 4) AS score_share_of_enrolled_at_year_month,
        RANK() OVER w_course_id_by_enrolled_at_yea_2 AS enrolled_at_year_rank,
        SUM(rows_in_student_id_course_id) OVER (PARTITION BY student_id, avg_score_tier ORDER BY avg_score_share_of_student_id_course_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rows_in_student_id_course_id_sum_2,
        ROUND(rows_in_student_id_course_id / NULLIF(SUM(rows_in_student_id_course_id) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_year)), 0), 4) AS rows_in_student_id_course_id_share_of_enrolled_a
    FROM ranked
    WINDOW w_enrolled_at_year_month AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_year)), w_course_id_by_enrolled_at_yea_2 AS (PARTITION BY course_id ORDER BY enrolled_at_year ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    avg_score_tier,
    student_id_course_id_min_avg_score,
    rows_in_student_id_course_id,
    avg_score_share_of_student_id_course_id,
    enrolled_at_year_cume_dist,
    second_rows_in_student_id_course_id,
    enrolled_at_year_rank,
    rows_in_student_id_course_id_sum_2,
    rows_in_student_id_course_id_share_of_enrolled_a,
    COALESCE(avg_score_tier, 'UNKNOWN') AS avg_score_tier_or_unknown
FROM with_trends
ORDER BY course_id

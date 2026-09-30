WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        CASE WHEN score >= 57.8 THEN 'large' WHEN score >= 38.3 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE student_id >= 4
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(term_grade_status_label), 1, 4) AS term_grade_status_label_prefix
    FROM cleaned
    WHERE TO_DATE(enrolled_at_quarter) < DATE '2025-02-09'
),
grade_status_stats AS (
    -- Avg score per grade_status
    SELECT grade_status, AVG(score) AS avg_score
    FROM enriched
    GROUP BY grade_status
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM enriched s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score > g.avg_score
),
grade_status_stats_2 AS (
    -- Avg avg_score per grade_status
    SELECT grade_status, AVG(avg_score) AS avg_avg_score
    FROM compared
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_avg_score,
        ROUND(s.avg_score - g.avg_avg_score, 2) AS avg_score_minus_avg
    FROM compared s
    JOIN grade_status_stats_2 g ON g.grade_status = s.grade_status
    WHERE s.avg_score > g.avg_avg_score
),
summary AS (
    -- Group-level totals
    SELECT
        grade_status,
        student_id,
        course_id,
        MAX(score) AS max_score,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        MIN(enrolled_at_quarter) AS first_enrolled_at_quarter,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM outliers
    GROUP BY grade_status, student_id, course_id
    HAVING SUM(score_minus_avg) > -30.0
),
metrics AS (
    -- Previous / next values per key
    SELECT
        grade_status,
        student_id,
        course_id,
        max_score,
        first_enrolled_at_quarter,
        term_list,
        ROUND(max_score / NULLIF(SUM(max_score) OVER (PARTITION BY distinct_grade_status, student_id), 0), 4) AS max_score_share_of_distinct_grade_status_student,
        MAX(distinct_grade_status) OVER (PARTITION BY distinct_grade_status, student_id) AS distinct_grade_status_student_id_max_distinct_gr,
        DENSE_RANK() OVER w_all_by_max_score AS max_score_dense_rank,
        DENSE_RANK() OVER (ORDER BY max_score ASC) AS max_score_dense_rank_2,
        SIZE(COLLECT_SET(grade_status) OVER w_distinct_grade_status_studen) AS distinct_grade_status_in_distinct_grade_status_s
    FROM summary
    WINDOW w_all_by_max_score AS (ORDER BY max_score ASC, grade_status, student_id, course_id), w_distinct_grade_status_studen AS (PARTITION BY distinct_grade_status, student_id)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        ROUND(AVG(score), 2) AS avg_enrollment_score,
        MAX(score) AS max_enrollment_score,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.avg_enrollment_score,
        e.max_enrollment_score,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM metrics s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT
    grade_status,
    student_id,
    course_id,
    max_score,
    max_score_share_of_distinct_grade_status_student,
    distinct_grade_status_student_id_max_distinct_gr,
    max_score_dense_rank,
    max_score_dense_rank_2,
    distinct_grade_status_in_distinct_grade_status_s,
    avg_enrollment_score,
    max_enrollment_score,
    enrollment_count,
    COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
    enrollment_count - max_score_dense_rank AS enrollment_count_minus_max_score_dense_rank
FROM with_counts
ORDER BY max_score_share_of_distinct_grade_status_student DESC, grade_status, student_id, course_id
LIMIT 20

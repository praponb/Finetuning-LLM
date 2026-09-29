WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE score >= 10.8
),
grade_status_stats AS (
    -- Median score per grade_status
    SELECT grade_status, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM base_enrollments
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM base_enrollments s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score >= g.median_score
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.median_score,
        s.score_minus_median,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM outliers s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(score_minus_median) OVER (ORDER BY score ASC, id, enrollment_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_minus_median_sum_2,
        SUM(score_minus_median) OVER (PARTITION BY enrollment_term, course_id ORDER BY score NULLS LAST, id, enrollment_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS score_minus_median_sum_3_prior,
        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY score_minus_median, id, enrollment_id) AS score_minus_median_seq,
        LEAD(enrollment_score, 3) OVER w_student_id_by_score_minus_me AS next3_enrollment_score,
        NTH_VALUE(median_score, 2) OVER (ORDER BY score ASC, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_median_score,
        ROUND(CUME_DIST() OVER w_all_by_score_ties, 3) AS score_cume_dist
    FROM with_children
    WINDOW w_student_id_by_score_minus_me AS (PARTITION BY student_id ORDER BY score_minus_median, id, enrollment_id), w_all_by_score_ties AS (ORDER BY score ASC)
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_id,
    score_minus_median_seq,
    course_id,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN enrollment_term = '2025T2' THEN 1 ELSE 0 END) AS n_enrollment_term_2025t2,
    ROUND(100 * COUNT(CASE WHEN enrollment_term = '2025T2' THEN 1 END) / COUNT(*), 1) AS pct_enrollment_term_2025t2
FROM with_trends
GROUP BY GROUPING SETS ((enrollment_id, score_minus_median_seq, course_id), (enrollment_id), (score_minus_median_seq), ())
ORDER BY enrollment_id NULLS LAST, score_minus_median_seq NULLS LAST, course_id NULLS LAST

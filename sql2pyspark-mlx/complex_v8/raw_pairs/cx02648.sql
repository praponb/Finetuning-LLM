WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE course_id >= 3
      AND score <= 83.1
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, SUM(score) AS total_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.total_enrollment_score
    FROM scoped_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
term_stats AS (
    -- Avg total_enrollment_score per term
    SELECT term, AVG(total_enrollment_score) AS avg_total_enrollment_score
    FROM summarized_rows
    GROUP BY term
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_total_enrollment_score,
        ROUND(s.total_enrollment_score - g.avg_total_enrollment_score, 2) AS total_enrollment_score_minus_avg
    FROM summarized_rows s
    JOIN term_stats g ON g.term = s.term
    WHERE s.total_enrollment_score > g.avg_total_enrollment_score
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref,
        ROUND(score * 0.9, 2) AS score_adj
    FROM outliers
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        enrolled_at_label,
        days_since_enrolled_at,
        total_enrollment_score,
        avg_total_enrollment_score,
        term_ref,
        score_adj,
        COUNT(*) OVER (PARTITION BY term_len) AS rows_in_term_len,
        ROUND(term_len / NULLIF(SUM(term_len) OVER w_term_len, 0), 4) AS term_len_share_of_term_len,
        RANK() OVER (PARTITION BY grade_status, term_len ORDER BY score ASC, id) AS score_rank,
        FIRST_VALUE(score) OVER (PARTITION BY student_id, grade_status ORDER BY total_enrollment_score_minus_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score
    FROM labeled
    WINDOW w_term_len AS (PARTITION BY term_len)
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score
    FROM ranked s
),
profiled_2 AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score_2,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score_3
    FROM profiled s
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    enrolled_at_label,
    days_since_enrolled_at,
    total_enrollment_score,
    term_ref,
    rows_in_term_len,
    term_len_share_of_term_len,
    score_rank,
    first_score,
    first_enrollment_enrolled_at,
    min_enrollment_score_3,
    CASE WHEN term IN ('2025T2', '2024T2') THEN 'group_a' WHEN term = '2025T1' THEN 'group_b' ELSE 'other' END AS term_group,
    FLOOR(first_score / 25) * 25 AS first_score_bucket
FROM profiled_2
ORDER BY rows_in_term_len DESC NULLS FIRST, enrolled_at_label DESC

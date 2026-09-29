WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 42.4) AS score_distance,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE score < 78.8
),
with_activity AS (
    -- Keep rows whose course matches
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM courses c
            WHERE c.id = s.course_id
              AND c.credits <= 3
        )
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 6
        )
),
latest AS (
    -- Keep the latest row per grade_status, student_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_distance,
        days_since_enrolled_at,
        grade_status_term_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY grade_status, student_id ORDER BY score_distance DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        grade_status,
        MIN(score) AS min_score,
        MIN(days_since_enrolled_at) AS min_days_since_enrolled_at,
        MAX(enrolled_at) AS last_enrolled_at,
        ROUND(100 * COUNT(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_incomplete,
        SUM(CASE WHEN grade_status <> 'PASS' THEN 1 ELSE 0 END) AS n_grade_status_pass,
        COUNT(DISTINCT id) AS distinct_ids
    FROM latest
    WHERE MONTH(enrolled_at) IN (1, 2)
    GROUP BY grade_status
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        grade_status,
        min_score,
        last_enrolled_at,
        pct_grade_status_incomplete,
        n_grade_status_pass,
        distinct_ids,
        ROUND(AVG(n_grade_status_pass) OVER (PARTITION BY grade_status, n_grade_status_pass ORDER BY last_enrolled_at DESC NULLS FIRST, grade_status ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_grade_status_pass_avg_7,
        NTH_VALUE(n_grade_status_pass, 2) OVER (PARTITION BY grade_status, n_grade_status_pass ORDER BY min_days_since_enrolled_at DESC, grade_status ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_grade_status_pass,
        FIRST_VALUE(distinct_ids) OVER (PARTITION BY grade_status, n_grade_status_pass ORDER BY last_enrolled_at DESC NULLS FIRST, grade_status ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_ids,
        SUM(distinct_ids) OVER w_grade_status AS grade_status_sum_distinct_ids
    FROM grouped
    WINDOW w_grade_status AS (PARTITION BY grade_status)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        first_distinct_ids - n_grade_status_pass AS first_distinct_ids_minus_n_grade_status_pass,
        LEAST(min_score, n_grade_status_pass) AS least_min_score_n_grade_status_pass,
        MONTH(last_enrolled_at) AS last_enrolled_at_month
    FROM windowed
)
-- Subtotals with CUBE
SELECT
    COALESCE(grade_status, 'ALL') AS grade_status,
    last_enrolled_at_month,
    first_distinct_ids,
    ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
    ROUND(SUM(min_score), 2) AS total_min_score,
    COUNT(*) AS cnt
FROM annotated
GROUP BY CUBE(grade_status, last_enrolled_at_month, first_distinct_ids)

WITH base_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score > 38.1
      AND enrolled_at < TIMESTAMP '2025-03-02 08:00:00'
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.credits AS course_credits,
        c.level AS course_level
    FROM base_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id AND c.course_code IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        course_credits,
        course_level,
        ROUND(course_credits / NULLIF(SUM(course_credits) OVER (PARTITION BY course_credits), 0), 4) AS course_credits_share_of_course_credits,
        RANK() OVER w_course_id_by_course_credits AS course_credits_rank,
        ROUND(score - AVG(score) OVER (PARTITION BY course_credits), 2) AS score_vs_course_credits_avg,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_credits), 2) AS course_credits_avg_course_credits
    FROM joined
    WINDOW w_course_id_by_course_credits AS (PARTITION BY course_id ORDER BY course_credits, id)
),
unique_rows AS (
    -- Keep the latest row per student_id, grade_status
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        course_credits,
        course_level,
        course_credits_share_of_course_credits,
        course_credits_rank,
        score_vs_course_credits_avg,
        course_credits_avg_course_credits
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY course_credits DESC, id DESC) AS recency_rank
        FROM sequenced
    ) ranked
    WHERE recency_rank = 1
)
SELECT id, field, metric_value
FROM (
    SELECT id, course_credits_rank, course_credits
    FROM unique_rows
) src
UNPIVOT (
    metric_value FOR field IN (course_credits_rank, course_credits)
) unp

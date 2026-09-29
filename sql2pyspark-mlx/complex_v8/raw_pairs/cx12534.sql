WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score >= 16.3
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_enrollments
    WHERE score < (SELECT MAX(score) FROM filtered_enrollments)
),
term_stats AS (
    -- Avg score per term
    SELECT term, AVG(score) AS avg_score
    FROM filtered
    GROUP BY term
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM filtered s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score > g.avg_score
),
term_stats_2 AS (
    -- Avg score_minus_avg per term
    SELECT term, AVG(score_minus_avg) AS avg_score_minus_avg
    FROM above_average
    GROUP BY term
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_minus_avg,
        ROUND(s.score_minus_avg - g.avg_score_minus_avg, 2) AS score_minus_avg_minus_avg
    FROM above_average s
    JOIN term_stats_2 g ON g.term = s.term
    WHERE s.score_minus_avg > g.avg_score_minus_avg
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        avg_score,
        score_minus_avg,
        avg_score_minus_avg,
        score_minus_avg_minus_avg,
        RANK() OVER (PARTITION BY grade_status ORDER BY score_minus_avg, id) AS score_minus_avg_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status ORDER BY score_minus_avg, id), 3) AS score_minus_avg_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY score_minus_avg, id) AS rn
    FROM vs_group
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        MIN(enrolled_at) AS first_enrollment_enrolled_at_2
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        e.first_enrollment_enrolled_at_2
    FROM sequenced s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        avg_score,
        avg_score_minus_avg,
        score_minus_avg_minus_avg,
        score_minus_avg_cume_dist,
        rn,
        first_enrollment_enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter,
        CASE WHEN term IN ('2025T1', '2025T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group,
        LOWER(TRIM(term)) AS term_clean
    FROM summarized_rows
    WHERE rn <= 8
)
SELECT
    id,
    course_id,
    term,
    score,
    enrolled_at,
    avg_score_minus_avg,
    score_minus_avg_minus_avg,
    score_minus_avg_cume_dist,
    first_enrollment_enrolled_at,
    enrolled_at_quarter,
    term_group,
    term_clean,
    ROUND(avg_score / NULLIF(score, 0), 3) AS avg_score_per_score
FROM annotated
ORDER BY score_minus_avg_minus_avg DESC

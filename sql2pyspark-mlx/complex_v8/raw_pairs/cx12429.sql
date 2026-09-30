WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 78.8 THEN 'HIGH' WHEN score >= 11.1 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label,
        CONCAT(grade_status, '#', CAST(student_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status = 'WITHDRAWN'
      AND HOUR(enrolled_at) BETWEEN 10 AND 15
),
score_tier_stats AS (
    -- Avg score per score_tier
    SELECT score_tier, AVG(score) AS avg_score
    FROM valid
    GROUP BY score_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM valid s
    JOIN score_tier_stats g ON g.score_tier = s.score_tier
    WHERE s.score > g.avg_score
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(score_tier), ' ', '_')) AS score_tier_slug,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown_2
    FROM compared
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        score,
        score_tier,
        enrolled_at_label,
        grade_status_ref,
        avg_score,
        grade_status_or_unknown,
        score_tier_slug,
        grade_status_or_unknown_2,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        RANK() OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id) AS enrolled_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY student_id, term ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist,
        NTILE(5) OVER (PARTITION BY grade_status_or_unknown_2, course_id ORDER BY score DESC, id) AS score_quintile,
        ROUND(avg_score - AVG(avg_score) OVER (PARTITION BY grade_status_or_unknown_2, score_tier), 2) AS avg_score_vs_grade_status_or_unknown_2_score_tie
    FROM annotated
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM windowed s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(avg_score) OVER (PARTITION BY grade_status_or_unknown, grade_status_or_unknown_2) AS grade_status_or_unknown_grade_status_or_unknown_,
        LAST_VALUE(avg_score) OVER (ORDER BY enrolled_at_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_avg_score,
        RANK() OVER (ORDER BY enrolled_at_rank, id) AS enrolled_at_rank_rank
    FROM with_stats
),
score_tier_stats_2 AS (
    -- Avg enrolled_at_rank_rank per score_tier
    SELECT score_tier, AVG(enrolled_at_rank_rank) AS avg_enrolled_at_rank_rank
    FROM windowed_2
    GROUP BY score_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_enrolled_at_rank_rank,
        ROUND(s.enrolled_at_rank_rank - g.avg_enrolled_at_rank_rank, 2) AS enrolled_at_rank_rank_minus_avg
    FROM windowed_2 s
    JOIN score_tier_stats_2 g ON g.score_tier = s.score_tier
    WHERE s.enrolled_at_rank_rank > g.avg_enrolled_at_rank_rank
)
SELECT
    id,
    student_id,
    score,
    score_tier,
    enrolled_at_label,
    grade_status_ref,
    avg_score,
    grade_status_or_unknown,
    score_tier_slug,
    grade_status_or_unknown_2,
    enrolled_at_rank,
    score_quintile,
    avg_score_vs_grade_status_or_unknown_2_score_tie,
    enrollment_count,
    last_avg_score,
    enrolled_at_rank_rank,
    avg_enrolled_at_rank_rank,
    enrolled_at_rank_rank_minus_avg,
    LOWER(REGEXP_REPLACE(TRIM(score_tier), ' ', '_')) AS score_tier_slug_2
FROM outliers
WHERE enrolled_at_rank_rank > 5
ORDER BY id DESC, avg_score NULLS LAST

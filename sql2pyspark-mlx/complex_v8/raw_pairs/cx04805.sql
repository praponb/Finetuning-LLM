WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 57.6 THEN 'HIGH' WHEN score >= 26.2 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        YEAR(enrolled_at) AS enrolled_at_year,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term = '2024T1'
      AND enrolled_at < TIMESTAMP '2025-02-24 14:00:00'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        course_id,
        ROUND(STDDEV(enrolled_at_year), 2) AS stddev_enrolled_at_year,
        COUNT(DISTINCT score_tier) AS distinct_score_tiers,
        ROUND(SUM(score_filled) / NULLIF(SUM(score), 0), 3) AS score_filled_to_score_ratio
    FROM cleaned_enrollments
    WHERE score BETWEEN 32.7 AND 83.1
    GROUP BY course_id
),
scored AS (
    -- Add calculated columns
    SELECT
        course_id,
        distinct_score_tiers,
        score_filled_to_score_ratio,
        COALESCE(stddev_enrolled_at_year, 0.0) AS stddev_enrolled_at_year_filled,
        ROUND(distinct_score_tiers / NULLIF(stddev_enrolled_at_year, 0), 3) AS distinct_score_tiers_per_stddev_enrolled_at_year,
        ABS(stddev_enrolled_at_year - 810.4) AS stddev_enrolled_at_year_distance
    FROM rollup_base
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM scored s
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.course_id,
        s.score_filled_to_score_ratio,
        s.stddev_enrolled_at_year_filled,
        s.distinct_score_tiers_per_stddev_enrolled_at_year,
        s.first_enrollment_enrolled_at,
        s.last_enrollment_enrolled_at,
        c.credits AS course_credits
    FROM profiled s
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    course_id,
    score_filled_to_score_ratio,
    stddev_enrolled_at_year_filled,
    distinct_score_tiers_per_stddev_enrolled_at_year,
    course_credits
FROM enriched
ORDER BY course_id, stddev_enrolled_at_year_filled DESC
LIMIT 5

WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 78.8 THEN 'HIGH' WHEN score >= 43.5 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        ABS(score - 50.9) AS score_distance
    FROM enrollments
    WHERE score > 30.0
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_tier,
        score_distance,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM filtered
    WHERE grade_status IN ('INCOMPLETE', 'PASS', 'FAIL')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        score_tier,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        COALESCE(score_distance, 0.0) AS score_distance_filled,
        LOWER(REGEXP_REPLACE(TRIM(score_tier), ' ', '_')) AS score_tier_slug
    FROM labeled
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.score_tier,
        s.grade_status_or_unknown,
        s.score_distance_filled,
        s.score_tier_slug,
        st.enrolled_on AS student_enrolled_on,
        st.student_name,
        st.gpa AS student_gpa
    FROM derived s
    LEFT JOIN students st ON st.id = s.student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM joined s
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score_distance_filled - AVG(score_distance_filled) OVER (PARTITION BY score_tier, term), 2) AS score_distance_filled_vs_score_tier_term_avg,
        ROUND(score_distance_filled / NULLIF(SUM(score_distance_filled) OVER w_score_tier_term, 0), 4) AS score_distance_filled_share_of_score_tier_term,
        ROUND(AVG(student_gpa) OVER (ORDER BY score_distance_filled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS student_gpa_avg_2_prior,
        ROUND(AVG(student_gpa) OVER (ORDER BY student_gpa DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS student_gpa_avg_3
    FROM with_stats
    WINDOW w_score_tier_term AS (PARTITION BY score_tier, term)
),
enrollment_stats_2 AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) AS enrollment_count,
        MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.min_enrollment_score
    FROM ranked s
    LEFT JOIN enrollment_stats_2 e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    id,
    term,
    score_tier,
    grade_status_or_unknown,
    score_distance_filled,
    score_tier_slug,
    student_enrolled_on,
    student_name,
    last_enrollment_enrolled_at,
    score_distance_filled_vs_score_tier_term_avg,
    score_distance_filled_share_of_score_tier_term,
    student_gpa_avg_2_prior,
    student_gpa_avg_3,
    enrollment_count,
    CASE WHEN score_tier IN ('LOW', 'MEDIUM') THEN 'group_a' WHEN score_tier = 'HIGH' THEN 'group_b' ELSE 'other' END AS score_tier_group,
    COALESCE(student_gpa_avg_2_prior, 0.0) AS student_gpa_avg_2_prior_filled
FROM profiled
ORDER BY score_tier_group NULLS LAST

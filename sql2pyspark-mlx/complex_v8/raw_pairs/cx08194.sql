WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 80.2 THEN 'A' WHEN score >= 42.4 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE student_id >= 9
      AND course_id <= 6
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, score_tier), 0), 4) AS score_share_of_grade_status_score_tier,
        ROUND(CUME_DIST() OVER (ORDER BY score NULLS LAST, id), 3) AS score_cume_dist,
        ROUND(score - AVG(score) OVER (PARTITION BY grade_status, score_tier), 2) AS score_vs_grade_status_score_tier_avg,
        MIN(score) OVER (PARTITION BY grade_status, score_tier) AS grade_status_score_tier_min_score,
        NTILE(10) OVER (PARTITION BY YEAR(enrolled_at) ORDER BY enrolled_at, id) AS enrolled_at_decile,
        SUM(score) OVER w_grade_status_score_tier AS grade_status_score_tier_sum_score
    FROM prepared
    WINDOW w_grade_status_score_tier AS (PARTITION BY grade_status, score_tier)
),
totals AS (
    -- Group-level totals
    SELECT
        student_id,
        enrolled_at_decile,
        MAX(enrolled_at) AS last_enrolled_at,
        SUM(grade_status_score_tier_sum_score) FILTER (WHERE grade_status = 'PASS') AS grade_status_score_tier_sum_score_grade_status_p,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        SUM(enrolled_at_decile) AS total_enrolled_at_decile,
        COUNT(*) AS row_count,
        ROUND(STDDEV(score_cume_dist), 2) AS stddev_score_cume_dist
    FROM with_windows
    GROUP BY student_id, enrolled_at_decile
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(enrolled_at_decile, 2) OVER (PARTITION BY row_count, student_id ORDER BY total_enrolled_at_decile DESC, student_id, enrolled_at_decile) AS prev2_enrolled_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY row_count, student_id ORDER BY total_enrolled_at_decile DESC), 3) AS total_enrolled_at_decile_percent_rank,
        DENSE_RANK() OVER w_student_id_by_total_enrolled AS total_enrolled_at_decile_dense_rank,
        MAX(row_count) OVER (PARTITION BY student_id ORDER BY total_enrolled_at_decile, student_id, enrolled_at_decile ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_count_max_3,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY row_count), 2) AS row_count_vs_row_count_avg,
        COUNT(*) OVER w_row_count AS rows_in_row_count
    FROM totals
    WINDOW w_student_id_by_total_enrolled AS (PARTITION BY student_id ORDER BY total_enrolled_at_decile), w_row_count AS (PARTITION BY row_count)
),
derived AS (
    -- Label and score rows
    SELECT
        student_id,
        enrolled_at_decile,
        row_count,
        stddev_score_cume_dist,
        prev2_enrolled_at_decile,
        total_enrolled_at_decile_percent_rank,
        total_enrolled_at_decile_dense_rank,
        DATE_TRUNC('WEEK', last_enrolled_at) AS last_enrolled_at_week,
        SIZE(grade_status_list) AS grade_status_list_count
    FROM sequenced
    WHERE enrolled_at_decile > 4
)
-- Final projection
SELECT
    student_id,
    enrolled_at_decile,
    stddev_score_cume_dist,
    prev2_enrolled_at_decile,
    total_enrolled_at_decile_percent_rank,
    total_enrolled_at_decile_dense_rank,
    last_enrolled_at_week,
    grade_status_list_count,
    CASE WHEN grade_status_list_count >= 2 THEN 'HIGH' WHEN grade_status_list_count >= 1 THEN 'MEDIUM' ELSE 'LOW' END AS grade_status_list_count_tier,
    last_enrolled_at_week + INTERVAL 7 DAYS AS last_enrolled_at_week_plus_7d
FROM derived
WHERE student_id IS NOT NULL
ORDER BY stddev_score_cume_dist, grade_status_list_count

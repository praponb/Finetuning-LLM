WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        ABS(score - 36.6) AS score_distance
    FROM enrollments
    WHERE score > 8.7
      AND grade_status <> 'WITHDRAWN'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_score AS rn,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        MIN(score) OVER (PARTITION BY course_id ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        COUNT(*) OVER (PARTITION BY MONTH(enrolled_at)) AS rows_in_enrolled_at_month,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(CUME_DIST() OVER w_all_by_score_distance, 3) AS score_distance_cume_dist,
        LAST_VALUE(score_distance) IGNORE NULLS OVER (ORDER BY score_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_distance,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        MIN(score_distance) OVER w_course_id AS course_id_min_score_distance,
        ROUND(AVG(score) OVER w_course_id, 2) AS course_id_avg_score,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id)) AS distinct_grade_status_in_course_id,
        LAG(enrolled_at, 3) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC NULLS FIRST, id) AS prev3_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2
    FROM filtered_enrollments
    WINDOW w_all_by_score AS (ORDER BY score, id), w_all_by_score_distance AS (ORDER BY score_distance, id), w_course_id AS (PARTITION BY course_id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        term_slug,
        score_distance,
        second_enrolled_at,
        min_score_so_far,
        rows_in_enrolled_at_month,
        course_id_min_score,
        first_enrolled_at,
        score_distance_cume_dist,
        last_known_score_distance,
        score_share_of_course_id,
        course_id_min_score_distance,
        course_id_avg_score,
        distinct_grade_status_in_course_id,
        prev3_enrolled_at,
        second_enrolled_at_2
    FROM with_trends
    WHERE rn = 1
      AND score_distance > 45.0
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(last_known_score_distance) OVER (PARTITION BY distinct_grade_status_in_course_id ORDER BY distinct_grade_status_in_course_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_known_score_distance,
        COUNT(*) OVER (PARTITION BY MONTH(second_enrolled_at_2)) AS rows_in_second_enrolled_at_2_month,
        LAG(score, 3) OVER (PARTITION BY distinct_grade_status_in_course_id ORDER BY distinct_grade_status_in_course_id, id) AS prev3_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(enrolled_at), course_id ORDER BY prev3_enrolled_at DESC, id), 3) AS prev3_enrolled_at_percent_rank,
        LAG(score_distance) OVER w_distinct_grade_status_in_cou_3 AS prev_score_distance,
        ROW_NUMBER() OVER (ORDER BY rows_in_enrolled_at_month, id) AS rn
    FROM top_per_group
    WINDOW w_distinct_grade_status_in_cou_3 AS (PARTITION BY distinct_grade_status_in_course_id, grade_status ORDER BY second_enrolled_at_2 ASC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        CASE WHEN score_distance_cume_dist >= 0.8 THEN 'HIGH' WHEN score_distance_cume_dist >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_distance_cume_dist_tier,
        YEAR(first_enrolled_at) AS first_enrolled_at_year,
        SUM(CASE WHEN grade_status = 'FAIL' THEN 1 ELSE 0 END) AS n_grade_status_fail,
        MIN(rows_in_second_enrolled_at_2_month) AS min_rows_in_second_enrolled_at_2_month,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        ROUND(STDDEV(course_id_min_score_distance), 2) AS stddev_course_id_min_score_distance
    FROM with_trends_2
    GROUP BY CASE WHEN score_distance_cume_dist >= 0.8 THEN 'HIGH' WHEN score_distance_cume_dist >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END, YEAR(first_enrolled_at)
)
-- Final projection
SELECT
    score_distance_cume_dist_tier,
    n_grade_status_fail,
    min_rows_in_second_enrolled_at_2_month,
    stddev_course_id_min_score_distance,
    COALESCE(first_enrolled_at_year, 0) AS first_enrolled_at_year_filled
FROM summary
ORDER BY score_distance_cume_dist_tier DESC NULLS FIRST

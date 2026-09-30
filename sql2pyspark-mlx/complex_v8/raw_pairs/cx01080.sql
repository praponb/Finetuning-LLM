WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE grade_status <> 'PASS'
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at_label,
        c.course_code,
        c.credits AS course_credits,
        c.level AS course_level
    FROM valid s
    JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at_label,
        course_code,
        course_credits,
        course_level,
        ROUND(AVG(score) OVER (PARTITION BY course_level), 2) AS course_level_avg_score,
        ROUND(AVG(course_credits) OVER (PARTITION BY course_level), 2) AS course_level_avg_course_credits,
        MIN(score) OVER (PARTITION BY term, course_id ORDER BY course_credits DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        SUM(score) OVER (PARTITION BY course_level) AS course_level_sum_score,
        score - LAG(score) OVER (ORDER BY score ASC, id) AS score_change,
        MIN(score) OVER w_course_level AS course_level_min_score,
        SUM(course_credits) OVER (PARTITION BY grade_status, course_id ORDER BY course_credits DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_course_credits,
        NTH_VALUE(score, 2) OVER (ORDER BY course_credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_level)) AS distinct_student_id_in_course_level,
        NTILE(4) OVER (ORDER BY score ASC, id) AS score_quartile,
        LAST_VALUE(course_credits) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_course_credits
    FROM joined
    WINDOW w_course_level AS (PARTITION BY course_level)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(course_code, '#', CAST(course_credits AS STRING)) AS course_code_ref,
        course_level_sum_score - last_course_credits AS course_level_sum_score_minus_last_course_credits,
        COALESCE(course_level, 'UNKNOWN') AS course_level_or_unknown
    FROM with_windows
    WHERE running_course_credits <= 30
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(course_level_sum_score, 2) OVER (ORDER BY course_level_sum_score_minus_last_course_credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_level_sum_score,
        COUNT(*) OVER w_course_level_or_unknown_cour AS rows_in_course_level_or_unknown_course_credits,
        NTH_VALUE(score_change, 2) OVER (ORDER BY course_level_min_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_change,
        ROUND(AVG(course_level_avg_score) OVER (ORDER BY course_level_sum_score_minus_last_course_credits ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS course_level_avg_score_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY score_quartile ORDER BY min_score_so_far, id) AS rn
    FROM annotated
    WINDOW w_course_level_or_unknown_cour AS (PARTITION BY course_level_or_unknown, course_credits)
),
stats AS (
    -- Group-level totals
    SELECT
        last_course_credits,
        ROUND(STDDEV(rn), 2) AS stddev_rn,
        ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list
    FROM sequenced
    GROUP BY last_course_credits
    HAVING MAX(course_level_sum_score_minus_last_course_credits) > 301.2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(stddev_rn) IGNORE NULLS OVER (PARTITION BY last_course_credits ORDER BY stddev_rn, last_course_credits ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stddev_rn,
        last_course_credits - LAG(last_course_credits) OVER (PARTITION BY last_course_credits ORDER BY stddev_rn, last_course_credits) AS last_course_credits_change
    FROM stats
)
-- Final projection
SELECT
    last_course_credits,
    stddev_rn,
    course_code_list,
    last_known_stddev_rn,
    last_course_credits_change,
    GREATEST(last_known_stddev_rn, stddev_rn) AS greatest_last_known_stddev_rn_stddev_rn,
    CASE WHEN stddev_rn >= 6.5 THEN 'large' WHEN stddev_rn >= 3.5 THEN 'medium' ELSE 'small' END AS stddev_rn_tier
FROM metrics
WHERE last_course_credits < 3
ORDER BY last_known_stddev_rn DESC NULLS FIRST

WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        MONTH(enrolled_at) AS enrolled_at_month,
        CASE WHEN score >= 56.1 THEN 'HIGH' WHEN score >= 43.5 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE score BETWEEN 16.3 AND 80.2
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        c.dept AS course_dept
    FROM valid s
    JOIN courses c ON c.id = s.course_id AND c.credits >= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_dept, student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        LAG(enrolled_at) OVER w_course_id_score_tier_by_enro AS prev_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_dept, student_id ORDER BY enrolled_at DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        NTILE(10) OVER (PARTITION BY course_id, score_tier ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        ROW_NUMBER() OVER w_score_tier_by_enrolled_at AS enrolled_at_seq,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, score_tier ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, score_tier ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_2,
        LAG(enrolled_at, 3) OVER w_score_tier_by_enrolled_at AS prev3_enrolled_at,
        LAG(enrolled_at) OVER (PARTITION BY course_dept, student_id ORDER BY enrolled_at DESC, id) AS prev_enrolled_at_2
    FROM joined
    WINDOW w_course_id_score_tier_by_enro AS (PARTITION BY course_id, score_tier ORDER BY enrolled_at DESC, id), w_score_tier_by_enrolled_at AS (PARTITION BY score_tier ORDER BY enrolled_at ASC, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        score_tier,
        course_dept,
        first_known_enrolled_at,
        second_enrolled_at,
        prev_enrolled_at,
        minutes_since_prev_enrolled_at,
        enrolled_at_decile,
        last_known_enrolled_at,
        last_enrolled_at,
        last_known_enrolled_at_2,
        prev3_enrolled_at,
        prev_enrolled_at_2
    FROM metrics
    WHERE enrolled_at_seq <= 3
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        course_dept,
        first_known_enrolled_at,
        second_enrolled_at,
        prev_enrolled_at,
        enrolled_at_decile,
        last_known_enrolled_at_2,
        prev_enrolled_at_2,
        MAX(enrolled_at_decile) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_enrolled_at_2)) AS last_known_enrolled_at_2_month_max_enrolled_at_d,
        SIZE(COLLECT_SET(course_id) OVER w_last_known_enrolled_at_2_mon) AS distinct_course_id_in_last_known_enrolled_at_2_m,
        COUNT(*) OVER (PARTITION BY enrolled_at_decile, student_id ORDER BY last_known_enrolled_at_2 ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(enrolled_at_decile - AVG(enrolled_at_decile) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_enrolled_at_2)), 2) AS enrolled_at_decile_vs_last_known_enrolled_at_2_m,
        RANK() OVER w_enrolled_at_decile_student_i_2 AS last_known_enrolled_at_2_rank,
        NTILE(4) OVER (ORDER BY minutes_since_prev_enrolled_at DESC NULLS FIRST, id) AS minutes_since_prev_enrolled_at_quartile,
        ROUND(enrolled_at_decile / NULLIF(SUM(enrolled_at_decile) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_enrolled_at_2)), 0), 4) AS enrolled_at_decile_share_of_last_known_enrolled_
    FROM leaders
    WINDOW w_last_known_enrolled_at_2_mon AS (PARTITION BY DATE_TRUNC('MONTH', last_known_enrolled_at_2)), w_enrolled_at_decile_student_i_2 AS (PARTITION BY enrolled_at_decile, student_id ORDER BY last_known_enrolled_at_2 ASC NULLS LAST, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        COUNT(*) FILTER (WHERE course_dept = 'Math') AS n_course_dept_math,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        COUNT(*) AS cnt,
        SUM(minutes_since_prev_enrolled_at_quartile) AS total_minutes_since_prev_enrolled_at_quartile,
        ROUND(SUM(distinct_course_id_in_last_known_enrolled_at_2_m) / NULLIF(SUM(enrolled_at_decile_vs_last_known_enrolled_at_2_m), 0), 3) AS distinct_course_id_in_last_known_enrolled_at_2_m_2,
        MAX(prev_enrolled_at) AS last_prev_enrolled_at
    FROM sequenced
    WHERE enrolled_at_decile_vs_last_known_enrolled_at_2_m > -5.0
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
)
SELECT
    enrolled_at_month,
    n_course_dept_math,
    grade_status_list,
    total_minutes_since_prev_enrolled_at_quartile,
    distinct_course_id_in_last_known_enrolled_at_2_m_2,
    last_prev_enrolled_at,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    ABS(cnt - 8) AS cnt_distance
FROM grouped
ORDER BY distinct_course_id_in_last_known_enrolled_at_2_m_2 NULLS LAST, enrolled_at_month

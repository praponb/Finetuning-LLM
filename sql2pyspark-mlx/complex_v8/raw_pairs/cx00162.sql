WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 3)
      AND course_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY grade_status_or_unknown, term ORDER BY score NULLS LAST, id) AS score_seq,
        SIZE(COLLECT_SET(grade_status_or_unknown) OVER (PARTITION BY student_id)) AS distinct_grade_status_or_unknown_in_student_id,
        NTH_VALUE(enrolled_at_plus_15m, 2) OVER (PARTITION BY grade_status_or_unknown, term ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_plus_15m,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        SIZE(COLLECT_SET(student_id) OVER w_grade_status_or_unknown) AS distinct_student_id_in_grade_status_or_unknown,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        MIN(score) OVER (PARTITION BY student_id) AS student_id_min_score,
        NTH_VALUE(enrolled_at_plus_15m, 2) OVER (PARTITION BY course_id, grade_status_or_unknown ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_plus_15m_2
    FROM recent_enrollments
    WINDOW w_grade_status_or_unknown AS (PARTITION BY grade_status_or_unknown)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at_plus_15m,
        grade_status_or_unknown,
        distinct_grade_status_or_unknown_in_student_id,
        second_enrolled_at_plus_15m,
        score_vs_student_id_avg,
        distinct_student_id_in_grade_status_or_unknown,
        student_id_avg_score,
        student_id_min_score,
        second_enrolled_at_plus_15m_2
    FROM windowed
    WHERE score_seq <= 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(score_vs_student_id_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', second_enrolled_at_plus_15m_2)) AS second_enrolled_at_plus_15m_2_month_max_score_vs,
        score - LAG(score) OVER w_student_id_by_score AS score_change,
        ROUND(student_id_min_score - AVG(student_id_min_score) OVER (PARTITION BY student_id), 2) AS student_id_min_score_vs_student_id_avg,
        MAX(student_id_min_score) OVER (PARTITION BY student_id) AS student_id_max_student_id_min_score,
        ROUND(student_id_avg_score - AVG(student_id_avg_score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score_vs_student_id_avg,
        ROUND(AVG(score_vs_student_id_avg) OVER w_student_id, 2) AS student_id_avg_score_vs_student_id_avg_2,
        LAG(score_vs_student_id_avg, 3, 0) OVER (PARTITION BY student_id ORDER BY score ASC, id) AS prev3_score_vs_student_id_avg,
        MAX(score_vs_student_id_avg) OVER (PARTITION BY student_id) AS student_id_max_score_vs_student_id_avg
    FROM first_rows
    WINDOW w_student_id_by_score AS (PARTITION BY student_id ORDER BY score ASC, id), w_student_id AS (PARTITION BY student_id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_student_id_in_grade_status_or_unknown,
        TO_DATE(enrolled_at_plus_15m) AS enrolled_at_plus_15m_day,
        CASE WHEN student_id_avg_score >= 85.0 THEN 'A' WHEN student_id_avg_score >= 20.0 THEN 'B' ELSE 'C' END AS student_id_avg_score_tier,
        COUNT(*) AS cnt,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MAX(student_id_avg_score_vs_student_id_avg) AS max_student_id_avg_score_vs_student_id_avg,
        COUNT(*) FILTER (WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'FAIL')) AS n_grade_status_in_incomplete_w
    FROM with_trends
    GROUP BY distinct_student_id_in_grade_status_or_unknown, TO_DATE(enrolled_at_plus_15m), CASE WHEN student_id_avg_score >= 85.0 THEN 'A' WHEN student_id_avg_score >= 20.0 THEN 'B' ELSE 'C' END
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(distinct_student_ids, 2) OVER (ORDER BY cnt ASC, distinct_student_id_in_grade_status_or_unknown, enrolled_at_plus_15m_day, student_id_avg_score_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_student_ids,
        NTILE(5) OVER (ORDER BY cnt ASC, distinct_student_id_in_grade_status_or_unknown, enrolled_at_plus_15m_day, student_id_avg_score_tier) AS cnt_quintile,
        COUNT(*) OVER (ORDER BY cnt ASC, distinct_student_id_in_grade_status_or_unknown, enrolled_at_plus_15m_day, student_id_avg_score_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (ORDER BY cnt ASC) AS cnt_rank
    FROM rollup_base
)
SELECT
    distinct_student_id_in_grade_status_or_unknown,
    distinct_student_ids,
    max_student_id_avg_score_vs_student_id_avg,
    second_distinct_student_ids,
    cnt_quintile,
    running_count,
    cnt_rank,
    LEAST(running_count, cnt_quintile) AS least_running_count_cnt_quintile
FROM sequenced
ORDER BY distinct_student_ids DESC

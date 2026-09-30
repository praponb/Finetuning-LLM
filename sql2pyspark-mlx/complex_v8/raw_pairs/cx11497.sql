WITH scoped_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('WITHDRAWN', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND student_id <= 8
),
combined_details_student AS (
    -- Join students
    SELECT s.*, st.major AS student_major
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_group,
        student_major,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY MONTH(enrolled_at), term), 0), 4) AS score_share_of_enrolled_at_month_term,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY MONTH(enrolled_at), term), 2) AS enrolled_at_month_term_avg_score,
        SIZE(COLLECT_SET(grade_status_group) OVER (PARTITION BY MONTH(enrolled_at), term)) AS distinct_grade_status_group_in_enrolled_at_month,
        LAG(score, 2) OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id) AS prev2_score,
        ROW_NUMBER() OVER w_grade_status_student_id_by_s_2 AS score_seq,
        ROUND(PERCENT_RANK() OVER w_grade_status_student_id_by_s_2, 3) AS score_percent_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY MONTH(enrolled_at), term)) AS distinct_grade_status_in_enrolled_at_month_term,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY student_id, grade_status ORDER BY score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3,
        ROUND(CUME_DIST() OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id), 3) AS score_cume_dist,
        LAG(score, 2, 0) OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id) AS prev2_score_2
    FROM combined_details_student
    WINDOW w_grade_status_student_id_by_s_2 AS (PARTITION BY grade_status, student_id ORDER BY score DESC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(score - 50.9) AS score_distance,
        score_avg_3 - prev2_score_2 AS score_avg_3_minus_prev2_score_2,
        COALESCE(score_avg_3, 0.0) AS score_avg_3_filled
    FROM sequenced
    WHERE student_id <= 8
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_grade_status_group_in_enrolled_at_month,
        SUM(CASE WHEN grade_status IN ('WITHDRAWN', 'INCOMPLETE') THEN 1 ELSE 0 END) AS n_grade_status_in_withdrawn_in,
        MAX(score_avg_3_filled) AS max_score_avg_3_filled,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM enriched
    GROUP BY 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_grade_status_in_withdrawn_in, distinct_grade_status_group_in_enrolled_at_month ORDER BY max_score_avg_3_filled ASC NULLS LAST, distinct_grade_status_group_in_enrolled_at_month), 3) AS max_score_avg_3_filled_percent_rank,
        LAST_VALUE(max_score_avg_3_filled) OVER (PARTITION BY n_grade_status_in_withdrawn_in, distinct_grade_status_group_in_enrolled_at_month ORDER BY max_score_avg_3_filled ASC NULLS LAST, distinct_grade_status_group_in_enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_score_avg_3_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY n_grade_status_in_withdrawn_in, distinct_grade_status_group_in_enrolled_at_month ORDER BY max_score_avg_3_filled ASC NULLS LAST, distinct_grade_status_group_in_enrolled_at_month), 3) AS max_score_avg_3_filled_cume_dist,
        RANK() OVER (PARTITION BY distinct_grade_status_group_in_enrolled_at_month ORDER BY max_score_avg_3_filled NULLS LAST, distinct_grade_status_group_in_enrolled_at_month) AS max_score_avg_3_filled_rank
    FROM rollup_base
)
-- Final projection
SELECT
    distinct_grade_status_group_in_enrolled_at_month,
    n_grade_status_in_withdrawn_in,
    max_score_avg_3_filled,
    term_list,
    max_score_avg_3_filled_percent_rank,
    last_max_score_avg_3_filled,
    max_score_avg_3_filled_cume_dist,
    max_score_avg_3_filled_rank,
    ABS(max_score_avg_3_filled - 60.0) AS max_score_avg_3_filled_distance
FROM ranked
WHERE distinct_grade_status_group_in_enrolled_at_month <= 8
ORDER BY n_grade_status_in_withdrawn_in NULLS LAST

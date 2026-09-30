WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE student_id <= 5
),
combined_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_hour,
        st.major AS student_major,
        st.student_name
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY student_major ORDER BY enrolled_at_hour DESC, id) AS enrolled_at_hour_rank,
        RANK() OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_student_major_by_enrolled_at)) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(AVG(enrolled_at_hour) OVER (PARTITION BY student_major, DATE_TRUNC('MONTH', enrolled_at)), 2) AS student_major_enrolled_at_month_avg_enrolled_at_,
        LEAD(enrolled_at_hour) OVER (ORDER BY enrolled_at DESC, id) AS next_enrolled_at_hour,
        COUNT(*) OVER (PARTITION BY student_major, DATE_TRUNC('MONTH', enrolled_at)) AS rows_in_student_major_enrolled_at_month,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY student_major, enrolled_at_hour)) AS distinct_grade_status_in_student_major_enrolled_,
        ROW_NUMBER() OVER (PARTITION BY student_major, course_id ORDER BY enrolled_at, id) AS rn
    FROM combined_details_student
    WINDOW w_student_major_by_enrolled_at AS (PARTITION BY student_major ORDER BY enrolled_at_hour DESC, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN student_major IN ('Art', 'CS') THEN 'group_a' WHEN student_major = 'Math' THEN 'group_b' ELSE 'other' END AS student_major_group
    FROM with_trends
    WHERE LOWER(student_name) LIKE '%lo%'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(rn) OVER (PARTITION BY distinct_grade_status_in_student_major_enrolled_, rn ORDER BY enrolled_at_hour ASC, id) AS prev_rn,
        COUNT(*) OVER w_student_major AS rows_in_student_major,
        RANK() OVER (PARTITION BY next_enrolled_at_hour, enrolled_at_hour ORDER BY enrolled_at_hour ASC, id) AS enrolled_at_hour_rank_2,
        SIZE(COLLECT_SET(student_major_group) OVER (PARTITION BY student_major)) AS distinct_student_major_group_in_student_major,
        enrolled_at_hour - LAG(enrolled_at_hour) OVER w_distinct_grade_status_in_stu AS enrolled_at_hour_change,
        rn - LAG(rn) OVER w_distinct_grade_status_in_stu AS rn_change
    FROM labeled
    WINDOW w_student_major AS (PARTITION BY student_major), w_distinct_grade_status_in_stu AS (PARTITION BY distinct_grade_status_in_student_major_enrolled_, rn ORDER BY enrolled_at_hour ASC, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        next_enrolled_at_hour,
        distinct_student_major_group_in_student_major,
        distinct_grade_status_in_student_major_enrolled_,
        COUNT(*) FILTER (WHERE grade_status IN ('WITHDRAWN', 'PASS', 'INCOMPLETE')) AS n_grade_status_in_withdrawn_pa,
        MIN(rows_in_student_major) AS min_rows_in_student_major,
        COUNT(*) AS cnt,
        SUM(CASE WHEN LOWER(student_name) LIKE '%a %' THEN 1 ELSE 0 END) AS n_lower_student_name_like_a,
        MIN(enrolled_at) AS first_enrolled_at,
        ROUND(SUM(rn_change) / NULLIF(SUM(enrolled_at_rank), 0), 3) AS rn_change_to_enrolled_at_rank_ratio
    FROM ranked
    GROUP BY next_enrolled_at_hour, distinct_student_major_group_in_student_major, distinct_grade_status_in_student_major_enrolled_
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(next_enrolled_at_hour - AVG(next_enrolled_at_hour) OVER (PARTITION BY distinct_student_major_group_in_student_major, next_enrolled_at_hour), 2) AS next_enrolled_at_hour_vs_distinct_student_major_,
        ROW_NUMBER() OVER (ORDER BY n_grade_status_in_withdrawn_pa, next_enrolled_at_hour, distinct_student_major_group_in_student_major, distinct_grade_status_in_student_major_enrolled_) AS rn,
        ROUND(PERCENT_RANK() OVER (ORDER BY n_grade_status_in_withdrawn_pa, next_enrolled_at_hour, distinct_student_major_group_in_student_major, distinct_grade_status_in_student_major_enrolled_), 3) AS n_grade_status_in_withdrawn_pa_percent_rank
    FROM rollup_base
)
-- Final projection
SELECT
    next_enrolled_at_hour,
    distinct_student_major_group_in_student_major,
    distinct_grade_status_in_student_major_enrolled_,
    n_grade_status_in_withdrawn_pa,
    cnt,
    n_lower_student_name_like_a,
    first_enrolled_at,
    rn_change_to_enrolled_at_rank_ratio,
    next_enrolled_at_hour_vs_distinct_student_major_,
    rn,
    n_grade_status_in_withdrawn_pa_percent_rank,
    CASE WHEN next_enrolled_at_hour >= 14 THEN 'HIGH' WHEN next_enrolled_at_hour >= 9 THEN 'MEDIUM' ELSE 'LOW' END AS next_enrolled_at_hour_tier
FROM with_trends_2
ORDER BY n_grade_status_in_withdrawn_pa_percent_rank DESC, next_enrolled_at_hour_vs_distinct_student_major_ DESC

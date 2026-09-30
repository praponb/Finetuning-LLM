WITH base AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        gpa,
        is_international
    FROM students
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.grade_status IS NOT NULL
              AND e.score <= 78.8
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN major IN ('CS', 'Bio') THEN 'group_a' WHEN major = 'Art' THEN 'group_b' ELSE 'other' END AS major_group
    FROM filtered
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_name,
        s.major,
        s.gpa,
        s.is_international,
        s.major_group,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM tagged s
    JOIN enrollments e ON e.student_id = s.id
),
grouped AS (
    -- Group-level totals
    SELECT
        enrollment_id,
        major_group,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list,
        MAX(enrollment_score) AS max_enrollment_score,
        COUNT(DISTINCT major_group) AS distinct_major_groups,
        COUNT(*) AS cnt,
        ROUND(SUM(gpa) / NULLIF(SUM(enrollment_score), 0), 3) AS gpa_to_enrollment_score_ratio
    FROM exploded_detail_enrollments
    GROUP BY enrollment_id, major_group
    HAVING MAX(gpa) > 2.5
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        enrollment_id,
        major_group,
        distinct_major_groups,
        cnt,
        gpa_to_enrollment_score_ratio,
        ROUND(distinct_major_groups - AVG(distinct_major_groups) OVER (PARTITION BY cnt, major_group), 2) AS distinct_major_groups_vs_cnt_major_group_avg,
        NTH_VALUE(max_enrollment_score, 2) OVER (PARTITION BY enrollment_id, cnt ORDER BY distinct_major_groups, enrollment_id, major_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_enrollment_score,
        RANK() OVER w_enrollment_id_cnt_by_distinc_2 AS distinct_major_groups_rank,
        MAX(cnt) OVER (PARTITION BY cnt, major_group) AS cnt_major_group_max_cnt,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY cnt, major_group), 0), 4) AS cnt_share_of_cnt_major_group
    FROM grouped
    WINDOW w_enrollment_id_cnt_by_distinc_2 AS (PARTITION BY enrollment_id, cnt ORDER BY distinct_major_groups, enrollment_id, major_group)
)
-- Final projection
SELECT
    enrollment_id,
    major_group,
    distinct_major_groups,
    cnt,
    gpa_to_enrollment_score_ratio,
    distinct_major_groups_vs_cnt_major_group_avg,
    second_max_enrollment_score,
    distinct_major_groups_rank,
    cnt_major_group_max_cnt
FROM with_trends
ORDER BY distinct_major_groups DESC

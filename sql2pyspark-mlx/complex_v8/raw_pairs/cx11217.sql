WITH scoped_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LENGTH(TRIM(student_name)) AS student_name_len,
        ABS(gpa - 3.2) AS gpa_distance
    FROM students
    WHERE NOT is_international
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(gpa_distance - AVG(gpa_distance) OVER (PARTITION BY major, student_name_len), 2) AS gpa_distance_vs_major_student_name_len_avg,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_on_ties, 3) AS enrolled_on_cume_dist,
        MAX(gpa_distance) OVER (ORDER BY student_name_len, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS gpa_distance_max_2_prior,
        COUNT(*) OVER (PARTITION BY major, is_international) AS rows_in_major_is_international,
        MIN(gpa_distance) OVER (PARTITION BY major, student_name_len) AS major_student_name_len_min_gpa_distance,
        SIZE(COLLECT_SET(major) OVER (PARTITION BY cohort)) AS distinct_major_in_cohort,
        SIZE(COLLECT_SET(cohort) OVER (PARTITION BY major, is_international)) AS distinct_cohort_in_major_is_international,
        COUNT(*) OVER (PARTITION BY cohort) AS rows_in_cohort
    FROM scoped_students
    WINDOW w_all_by_enrolled_on_ties AS (ORDER BY enrolled_on)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY major, cohort ORDER BY gpa_distance DESC, id) AS seq_no,
        RANK() OVER (PARTITION BY distinct_major_in_cohort ORDER BY gpa DESC NULLS FIRST) AS gpa_rank,
        NTILE(3) OVER (PARTITION BY distinct_major_in_cohort ORDER BY gpa DESC NULLS FIRST, id) AS gpa_tercile,
        LAG(enrolled_on_cume_dist, 2) OVER (PARTITION BY major, cohort ORDER BY gpa_distance DESC, id) AS prev2_enrolled_on_cume_dist,
        LEAD(gpa, 2) OVER (PARTITION BY distinct_major_in_cohort ORDER BY gpa DESC NULLS FIRST, id) AS next2_gpa,
        COUNT(*) OVER (PARTITION BY student_name_len ORDER BY gpa_distance_vs_major_student_name_len_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(gpa) OVER (PARTITION BY major, cohort ORDER BY gpa_distance DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_4,
        MIN(gpa) OVER (PARTITION BY distinct_major_in_cohort, distinct_cohort_in_major_is_international) AS distinct_major_in_cohort_distinct_cohort_in_majo
    FROM windowed
)
SELECT
    id,
    student_name,
    major,
    enrolled_on,
    gpa,
    is_international,
    student_name_len,
    gpa_distance_vs_major_student_name_len_avg,
    enrolled_on_cume_dist,
    rows_in_major_is_international,
    major_student_name_len_min_gpa_distance,
    distinct_major_in_cohort,
    distinct_cohort_in_major_is_international,
    rows_in_cohort,
    seq_no,
    gpa_rank,
    prev2_enrolled_on_cume_dist,
    next2_gpa,
    running_count,
    gpa_avg_4,
    distinct_major_in_cohort_distinct_cohort_in_majo
FROM sequenced
ORDER BY next2_gpa DESC, rows_in_cohort DESC NULLS FIRST, id
LIMIT 20

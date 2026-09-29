WITH cleaned_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        ABS(gpa - 2.7) AS gpa_distance,
        DATEDIFF(DATE '2025-06-30', enrolled_on) AS days_since_enrolled_on,
        DATEDIFF(DATE '2025-06-30', enrolled_on) AS days_since_enrolled_on_2
    FROM students
),
stats AS (
    -- Group-level totals
    SELECT
        major,
        MIN(enrolled_on) AS first_enrolled_on,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT major) AS distinct_majors,
        ROUND(SUM(gpa) / NULLIF(SUM(gpa_distance), 0), 3) AS gpa_to_gpa_distance_ratio
    FROM cleaned_students
    GROUP BY major
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_majors > (SELECT AVG(distinct_majors) FROM stats)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        major,
        distinct_majors,
        ROUND(n_rows - AVG(n_rows) OVER w_n_rows, 2) AS n_rows_vs_n_rows_avg,
        ROUND(AVG(distinct_majors) OVER (PARTITION BY distinct_majors, major ORDER BY gpa_to_gpa_distance_ratio ASC, major ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS distinct_majors_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY distinct_majors, major ORDER BY gpa_to_gpa_distance_ratio ASC, major) AS gpa_to_gpa_distance_ratio_seq
    FROM without_matches
    WINDOW w_n_rows AS (PARTITION BY n_rows)
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(n_rows_vs_n_rows_avg, 0.0) AS n_rows_vs_n_rows_avg_filled
    FROM with_trends
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        major,
        n_rows_vs_n_rows_avg,
        n_rows_vs_n_rows_avg_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY gpa_to_gpa_distance_ratio_seq, distinct_majors ORDER BY distinct_majors DESC), 3) AS distinct_majors_cume_dist,
        FIRST_VALUE(n_rows_vs_n_rows_avg_filled) OVER (PARTITION BY gpa_to_gpa_distance_ratio_seq, distinct_majors ORDER BY distinct_majors DESC, major ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_rows_vs_n_rows_avg_filled,
        NTILE(10) OVER (PARTITION BY gpa_to_gpa_distance_ratio_seq, distinct_majors ORDER BY distinct_majors DESC, major) AS distinct_majors_decile
    FROM scored
),
annotated AS (
    -- Label and score rows
    SELECT
        major,
        n_rows_vs_n_rows_avg,
        n_rows_vs_n_rows_avg_filled,
        distinct_majors_cume_dist,
        first_n_rows_vs_n_rows_avg_filled,
        distinct_majors_decile,
        ROUND(distinct_majors_decile / NULLIF(n_rows_vs_n_rows_avg, 0), 3) AS distinct_majors_decile_per_n_rows_vs_n_rows_avg,
        LENGTH(TRIM(major)) AS major_len
    FROM with_trends_2
    WHERE first_n_rows_vs_n_rows_avg_filled >= -8.4
)
-- Final projection
SELECT
    major,
    n_rows_vs_n_rows_avg,
    n_rows_vs_n_rows_avg_filled,
    distinct_majors_cume_dist,
    first_n_rows_vs_n_rows_avg_filled,
    distinct_majors_decile_per_n_rows_vs_n_rows_avg,
    major_len,
    CASE WHEN major IN ('Bio', 'Math') THEN 'group_a' WHEN major = 'CS' THEN 'group_b' ELSE 'other' END AS major_group
FROM annotated
WHERE major_len < 11
ORDER BY n_rows_vs_n_rows_avg, major_len DESC NULLS FIRST

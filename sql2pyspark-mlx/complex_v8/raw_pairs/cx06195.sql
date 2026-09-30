WITH scoped AS (
    -- Filter students
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        is_international,
        ROUND(gpa * 1.5, 2) AS gpa_adj,
        LENGTH(TRIM(student_name)) AS student_name_len
    FROM students
    WHERE is_international = TRUE
      AND cohort = 'C2023'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        is_international,
        student_name_len,
        ROUND(student_name_len / NULLIF(SUM(student_name_len) OVER (PARTITION BY student_name_len, is_international), 0), 4) AS student_name_len_share_of_student_name_len_is_in,
        MAX(gpa) OVER w_student_name_len_is_internat AS student_name_len_is_international_max_gpa,
        MAX(gpa_adj) OVER (PARTITION BY student_name_len ORDER BY student_name_len, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS gpa_adj_max_1_prior,
        NTH_VALUE(gpa, 2) OVER (ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY student_name_len, is_international), 2) AS gpa_vs_student_name_len_is_international_avg
    FROM scoped
    WINDOW w_student_name_len_is_internat AS (PARTITION BY student_name_len, is_international)
),
filtered AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.term = '2025T2'
        )
)
SELECT
    id,
    enrolled_on,
    gpa,
    student_name_len,
    student_name_len_is_international_max_gpa,
    second_gpa,
    gpa_vs_student_name_len_is_international_avg,
    IF(is_international, 'yes', 'no') AS is_international_label,
    GREATEST(second_gpa, student_name_len) AS greatest_second_gpa_student_name_len
FROM filtered
WHERE student_name_len < 10
ORDER BY id DESC, student_name_len_is_international_max_gpa DESC

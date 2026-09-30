WITH recent AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE cohort <> 'C2022'
      AND UPPER(TRIM(student_name)) LIKE 'D%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        ROUND(AVG(gpa) OVER w_is_international, 2) AS is_international_avg_gpa,
        RANK() OVER w_all_by_gpa_ties AS gpa_rank,
        ROUND(AVG(gpa) OVER (PARTITION BY is_international, major ORDER BY enrolled_on DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS gpa_avg_3,
        MIN(gpa) OVER (PARTITION BY is_international) AS is_international_min_gpa
    FROM recent
    WINDOW w_is_international AS (PARTITION BY is_international), w_all_by_gpa_ties AS (ORDER BY gpa ASC)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MIN(enrolled_at) AS first_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count,
        ROUND(AVG(score), 2) AS avg_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.first_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count,
        e.avg_enrollment_score
    FROM sequenced s
    LEFT JOIN enrollment_stats e ON e.student_id = s.id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count_2
    FROM summarized_rows s
),
grouped AS (
    -- Summarise per group
    SELECT
        major,
        enrollment_count_2,
        gpa_rank,
        COUNT(*) AS cnt,
        DATEDIFF(MAX(enrolled_on), MIN(enrolled_on)) AS enrolled_on_span_days
    FROM with_counts
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
)
SELECT
    major,
    enrollment_count_2,
    gpa_rank,
    cnt
FROM grouped
ORDER BY cnt DESC

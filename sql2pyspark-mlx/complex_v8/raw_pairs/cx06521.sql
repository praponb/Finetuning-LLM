WITH valid AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE gpa < 3.4
),
screened AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.id
              AND e.term = '2025T2'
        )
      AND LENGTH(TRIM(s.student_name)) >= 6
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(gpa) OVER (ORDER BY gpa, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS gpa_max_6_prior,
        FIRST_VALUE(gpa) OVER (ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_gpa,
        ROUND(PERCENT_RANK() OVER (ORDER BY gpa, id), 3) AS gpa_percent_rank,
        NTH_VALUE(enrolled_on, 2) OVER (ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on,
        RANK() OVER w_all_by_gpa_ties AS gpa_rank
    FROM screened
    WINDOW w_all_by_gpa_ties AS (ORDER BY gpa)
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS first_enrollment_enrolled_at,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.id) AS max_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count
    FROM metrics s
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(gpa_max_6_prior) OVER w_gpa_rank_enrollment_count_by AS next_gpa_max_6_prior,
        COUNT(*) OVER (PARTITION BY gpa_rank, cohort) AS rows_in_gpa_rank_cohort,
        ROUND(AVG(gpa_max_6_prior) OVER (ORDER BY first_enrollment_enrolled_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS gpa_max_6_prior_avg_4
    FROM with_counts
    WINDOW w_gpa_rank_enrollment_count_by AS (PARTITION BY gpa_rank, enrollment_count ORDER BY gpa, id)
),
cohort_stats AS (
    -- Max gpa_max_6_prior per cohort
    SELECT cohort, MAX(gpa_max_6_prior) AS max_gpa_max_6_prior
    FROM with_trends
    GROUP BY cohort
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_gpa_max_6_prior,
        ROUND(s.gpa_max_6_prior - g.max_gpa_max_6_prior, 2) AS gpa_max_6_prior_minus_max
    FROM with_trends s
    JOIN cohort_stats g ON g.cohort = s.cohort
    WHERE s.gpa_max_6_prior = g.max_gpa_max_6_prior
)
SELECT *
FROM (
    SELECT enrolled_on, cohort, rows_in_gpa_rank_cohort
    FROM compared
) src
PIVOT (
    AVG(rows_in_gpa_rank_cohort)
    FOR cohort IN ('C2021', 'C2023')
)
ORDER BY enrolled_on

WITH valid_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE gpa >= 2.7
      AND cohort = 'C2024'
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.id) AS max_enrollment_score
    FROM valid_students s
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(enrolled_on) AS enrolled_on_year,
        MAX(enrolled_on) AS last_enrolled_on,
        ROUND(AVG(max_enrollment_score), 2) AS avg_max_enrollment_score,
        PERCENTILE_APPROX(max_enrollment_score, 0.5) AS median_max_enrollment_score,
        COUNT(*) AS cnt,
        SUM(gpa) AS total_gpa
    FROM with_stats
    GROUP BY YEAR(enrolled_on)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(last_enrolled_on, LAG(last_enrolled_on) OVER (PARTITION BY enrolled_on_year, cnt ORDER BY median_max_enrollment_score DESC, enrolled_on_year)) AS days_since_prev_last_enrolled_on,
        SUM(avg_max_enrollment_score) OVER (PARTITION BY enrolled_on_year, cnt ORDER BY median_max_enrollment_score DESC, enrolled_on_year ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_max_enrollment_score_sum_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY enrolled_on_year, cnt ORDER BY median_max_enrollment_score DESC), 3) AS median_max_enrollment_score_cume_dist
    FROM grouped
)
SELECT enrolled_on_year, field, value
FROM (
    SELECT enrolled_on_year, median_max_enrollment_score_cume_dist, avg_max_enrollment_score
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (median_max_enrollment_score_cume_dist, avg_max_enrollment_score)
) unp

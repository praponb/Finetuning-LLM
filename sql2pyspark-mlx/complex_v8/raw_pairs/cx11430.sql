WITH valid AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        ROUND(gpa * 0.9, 2) AS gpa_adj
    FROM students
    WHERE gpa BETWEEN 2.7 AND 3.3
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        cohort,
        COUNT(*) AS n_rows,
        COUNT(*) FILTER (WHERE major = 'Bio') AS n_major_bio,
        MIN(enrolled_on) AS first_enrolled_on
    FROM valid
    GROUP BY cohort
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        cohort,
        n_major_bio,
        first_enrolled_on,
        LEAD(first_enrolled_on) OVER (PARTITION BY n_major_bio ORDER BY first_enrolled_on, cohort) AS next_first_enrolled_on,
        SUM(n_major_bio) OVER (PARTITION BY n_rows, n_major_bio) AS n_rows_n_major_bio_sum_n_major_bio
    FROM grouped
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        cohort,
        first_enrolled_on,
        LEAD(first_enrolled_on, 3) OVER (PARTITION BY cohort ORDER BY first_enrolled_on DESC, cohort) AS next3_first_enrolled_on,
        ROUND(AVG(n_major_bio) OVER (PARTITION BY cohort ORDER BY first_enrolled_on DESC, cohort ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_major_bio_avg_7,
        ROUND(n_major_bio / NULLIF(SUM(n_major_bio) OVER (PARTITION BY n_major_bio, cohort), 0), 4) AS n_major_bio_share_of_n_major_bio_cohort,
        LAST_VALUE(next_first_enrolled_on) OVER (PARTITION BY n_major_bio ORDER BY first_enrolled_on DESC, cohort ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_next_first_enrolled_on,
        LAG(n_major_bio, 2) OVER (PARTITION BY cohort ORDER BY first_enrolled_on DESC, cohort) AS prev2_n_major_bio,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_major_bio ORDER BY first_enrolled_on DESC, cohort), 3) AS first_enrolled_on_percent_rank
    FROM with_trends
),
derived AS (
    -- Label and score rows
    SELECT
        cohort,
        first_enrolled_on,
        next3_first_enrolled_on,
        n_major_bio_avg_7,
        n_major_bio_share_of_n_major_bio_cohort,
        prev2_n_major_bio,
        first_enrolled_on_percent_rank,
        ROUND(prev2_n_major_bio / NULLIF(n_major_bio_avg_7, 0), 3) AS prev2_n_major_bio_per_n_major_bio_avg_7
    FROM windowed
    WHERE n_major_bio_avg_7 >= 3.5
)
-- Subtotals with GROUPING SETS
SELECT
    cohort,
    prev2_n_major_bio,
    ROUND(AVG(n_major_bio_share_of_n_major_bio_cohort), 2) AS avg_n_major_bio_share_of_n_major_bio_cohort,
    COUNT(*) FILTER (WHERE cohort <> 'C2024') AS n_cohort_c2024
FROM derived
GROUP BY GROUPING SETS ((cohort, prev2_n_major_bio), (cohort), ())
ORDER BY cohort NULLS LAST, prev2_n_major_bio NULLS LAST

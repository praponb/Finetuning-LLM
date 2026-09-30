WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND course_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per student_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        term_prefix,
        enrolled_at_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id DESC) AS rn
        FROM scoped_enrollments
    ) ranked
    WHERE rn = 1
),
screened AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.is_international = FALSE
              AND st.major IS NOT NULL
        )
),
with_details_course AS (
    -- Join courses
    SELECT s.*, c.credits AS course_credits, st.cohort AS student_cohort
    FROM screened s
    JOIN courses c ON c.id = s.course_id AND c.level = 'core'
    LEFT JOIN students st ON st.id = s.student_id
),
term_stats AS (
    -- Max score per term
    SELECT term, MAX(score) AS max_score
    FROM with_details_course
    GROUP BY term
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM with_details_course s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score = g.max_score
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(enrolled_at_dayofweek - AVG(enrolled_at_dayofweek) OVER (PARTITION BY course_id), 2) AS enrolled_at_dayofweek_vs_course_id_avg,
        FIRST_VALUE(max_score) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_score
    FROM vs_group
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_dayofweek,
        CASE WHEN score >= 77.0 THEN 'HIGH' WHEN score >= 13.9 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        YEAR(enrolled_at) AS enrolled_at_year,
        MAX(score_minus_max) AS max_score_minus_max,
        SUM(CASE WHEN student_cohort = 'C2024' THEN 1 ELSE 0 END) AS n_student_cohort_c2024,
        MIN(enrolled_at_dayofweek) AS min_enrolled_at_dayofweek
    FROM sequenced
    GROUP BY enrolled_at_dayofweek, CASE WHEN score >= 77.0 THEN 'HIGH' WHEN score >= 13.9 THEN 'MEDIUM' ELSE 'LOW' END, YEAR(enrolled_at)
)
SELECT
    enrolled_at_dayofweek,
    score_tier,
    enrolled_at_year,
    n_student_cohort_c2024,
    min_enrolled_at_dayofweek,
    SUBSTRING(TRIM(score_tier), 1, 3) AS score_tier_prefix,
    GREATEST(enrolled_at_year, n_student_cohort_c2024) AS greatest_enrolled_at_year_n_student_cohort_c2024
FROM rollup_base
WHERE max_score_minus_max < 80.0
ORDER BY score_tier_prefix

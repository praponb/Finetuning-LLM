WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-13 09:00:00'
),
enriched AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        st.major AS student_major,
        st.cohort AS student_cohort
    FROM recent_enrollments s
    JOIN students st ON st.id = s.student_id AND st.cohort IS NOT NULL
    WHERE st.major IN ('CS', 'Art', 'Bio')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(score) OVER (PARTITION BY student_id ORDER BY enrolled_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_max_2,
        NTILE(5) OVER w_student_id_by_enrolled_at AS enrolled_at_quintile,
        ROUND(score / NULLIF(SUM(score) OVER w_term, 0), 4) AS score_share_of_term,
        ROUND(score - AVG(score) OVER (PARTITION BY term, student_cohort), 2) AS score_vs_term_student_cohort_avg,
        ROW_NUMBER() OVER w_student_cohort_student_id_by AS rn,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_cohort, course_id ORDER BY score ASC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(score - AVG(score) OVER (PARTITION BY student_cohort), 2) AS score_vs_student_cohort_avg,
        MIN(score) OVER w_term AS term_min_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score
    FROM enriched
    WINDOW w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at DESC, id), w_term AS (PARTITION BY term), w_student_cohort_student_id_by AS (PARTITION BY student_cohort, student_id ORDER BY enrolled_at, id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        MIN(enrolled_at) AS first_enrolled_at,
        ARRAY_SORT(COLLECT_SET(student_cohort)) AS student_cohort_list,
        SUM(CASE WHEN student_cohort = 'C2023' THEN 1 ELSE 0 END) AS n_student_cohort_c2023,
        ROUND(AVG(enrolled_at_quintile), 2) AS avg_enrolled_at_quintile,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN term = '2024T2' THEN 1 END) / COUNT(*), 1) AS pct_term_2024t2
    FROM windowed
    WHERE minutes_since_prev_enrolled_at <= 3500.0
    GROUP BY DATE_TRUNC('MONTH', enrolled_at)
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY n_student_cohort_c2023, row_count ORDER BY avg_enrolled_at_quintile DESC, enrolled_at_month) AS avg_enrolled_at_quintile_tercile,
        ROUND(avg_enrolled_at_quintile / NULLIF(SUM(avg_enrolled_at_quintile) OVER (PARTITION BY row_count), 0), 4) AS avg_enrolled_at_quintile_share_of_row_count
    FROM rollup_base
)
SELECT
    enrolled_at_month,
    first_enrolled_at,
    student_cohort_list,
    n_student_cohort_c2023,
    avg_enrolled_at_quintile,
    row_count,
    pct_term_2024t2,
    avg_enrolled_at_quintile_tercile,
    ROUND(row_count / NULLIF(avg_enrolled_at_quintile, 0), 3) AS row_count_per_avg_enrolled_at_quintile,
    MONTH(enrolled_at_month) AS enrolled_at_month_month
FROM with_windows
WHERE avg_enrolled_at_quintile_tercile BETWEEN 3 AND 7
ORDER BY avg_enrolled_at_quintile_tercile DESC

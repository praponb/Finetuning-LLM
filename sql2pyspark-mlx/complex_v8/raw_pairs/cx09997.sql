WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE score BETWEEN 4.9 AND 69.6
      AND grade_status IS NOT NULL
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at_date,
        st.gpa AS student_gpa,
        c.course_code
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id AND st.enrolled_on < DATE '2025-05-07'
    LEFT JOIN courses c ON c.id = s.course_id
    WHERE st.gpa <= 3.2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY grade_status, course_id) AS rows_in_grade_status_course_id,
        ROUND(AVG(score) OVER (ORDER BY score DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS score_avg_3_prior,
        SIZE(COLLECT_SET(course_id) OVER w_enrolled_at_date_year) AS distinct_course_id_in_enrolled_at_date_year,
        ROW_NUMBER() OVER (ORDER BY score DESC, id) AS score_seq,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY YEAR(enrolled_at_date))) AS distinct_grade_status_in_enrolled_at_date_year,
        ROUND(AVG(student_gpa) OVER (PARTITION BY grade_status, course_id), 2) AS grade_status_course_id_avg_student_gpa,
        LAST_VALUE(student_gpa) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_student_gpa,
        ROUND(student_gpa - AVG(student_gpa) OVER (PARTITION BY grade_status, course_id), 2) AS student_gpa_vs_grade_status_course_id_avg,
        DENSE_RANK() OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at_date) AS enrolled_at_date_dense_rank,
        MAX(score) OVER w_enrolled_at_date_year AS enrolled_at_date_year_max_score,
        FIRST_VALUE(student_gpa) IGNORE NULLS OVER (ORDER BY student_gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_student_gpa,
        ROUND(PERCENT_RANK() OVER (ORDER BY score DESC), 3) AS score_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_student_gpa_ties, 3) AS student_gpa_cume_dist,
        NTILE(5) OVER (PARTITION BY MONTH(enrolled_at_date) ORDER BY enrolled_at_date, id) AS enrolled_at_date_quintile,
        FIRST_VALUE(score) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score
    FROM combined_details
    WINDOW w_enrolled_at_date_year AS (PARTITION BY YEAR(enrolled_at_date)), w_all_by_score AS (ORDER BY score DESC, id), w_all_by_student_gpa_ties AS (ORDER BY student_gpa ASC)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score_avg_3_prior - AVG(score_avg_3_prior) OVER (PARTITION BY distinct_grade_status_in_enrolled_at_date_year, score_seq), 2) AS score_avg_3_prior_vs_distinct_grade_status_in_en,
        NTILE(5) OVER w_distinct_course_id_in_enroll AS first_score_quintile,
        LAG(score_percent_rank) OVER (ORDER BY enrolled_at_date_dense_rank, id) AS prev_score_percent_rank,
        LAG(score_seq) OVER w_enrolled_at_date_quintile_by AS prev_score_seq
    FROM metrics
    WINDOW w_distinct_course_id_in_enroll AS (PARTITION BY distinct_course_id_in_enrolled_at_date_year ORDER BY first_score, id), w_enrolled_at_date_quintile_by AS (PARTITION BY enrolled_at_date_quintile ORDER BY enrolled_at_date_year_max_score DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        student_id,
        first_score_quintile,
        SUM(score_percent_rank) FILTER (WHERE LENGTH(TRIM(course_code)) >= 7) AS score_percent_rank_length_trim_course_code_7,
        ROUND(SUM(grade_status_course_id_avg_student_gpa) / NULLIF(SUM(student_gpa), 0), 3) AS grade_status_course_id_avg_student_gpa_to_studen,
        DATEDIFF(MAX(enrolled_at_date), MIN(enrolled_at_date)) AS enrolled_at_date_span_days,
        PERCENTILE_APPROX(student_gpa, 0.5) AS median_student_gpa
    FROM sequenced
    GROUP BY 1, 2
    HAVING MAX(grade_status_course_id_avg_student_gpa) > 2.1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        student_id,
        first_score_quintile,
        median_student_gpa,
        MAX(first_score_quintile) OVER (PARTITION BY student_id, first_score_quintile) AS student_id_first_score_quintile_max_first_score_,
        COUNT(*) OVER (PARTITION BY student_id, first_score_quintile) AS rows_in_student_id_first_score_quintile
    FROM totals
)
SELECT
    student_id,
    student_id_first_score_quintile_max_first_score_,
    rows_in_student_id_first_score_quintile,
    ABS(first_score_quintile - 5) AS first_score_quintile_distance
FROM windowed
ORDER BY student_id DESC, first_score_quintile_distance

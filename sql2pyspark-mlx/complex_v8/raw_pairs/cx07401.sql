WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE student_id >= 19
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        LEAD(enrolled_at) OVER w_all_by_score AS next_enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER w_enrolled_at_to_date_course_i, 0), 4) AS score_share_of_enrolled_at_to_date_course_id,
        ROUND(score - AVG(score) OVER w_student_id, 2) AS score_vs_student_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at) ORDER BY score, id), 3) AS score_percent_rank,
        SIZE(COLLECT_SET(student_id) OVER w_enrolled_at_to_date_course_i) AS distinct_student_id_in_enrolled_at_to_date_cours,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS score_avg_2_prior,
        SUM(score) OVER w_student_id AS student_id_sum_score,
        MAX(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3
    FROM cleaned_enrollments
    WINDOW w_all_by_score AS (ORDER BY score ASC, id), w_enrolled_at_to_date_course_i AS (PARTITION BY TO_DATE(enrolled_at), course_id), w_student_id AS (PARTITION BY student_id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        score_vs_student_id_avg,
        score_percent_rank,
        distinct_student_id_in_enrolled_at_to_date_cours,
        score_share_of_student_id,
        student_id_sum_score,
        score_max_3,
        DATE_FORMAT(next_enrolled_at, 'MMM yyyy') AS next_enrolled_at_label,
        FLOOR(score_vs_student_id_avg / 25) * 25 AS score_vs_student_id_avg_bucket,
        CASE WHEN score >= 77.0 THEN 'large' WHEN score >= 16.3 THEN 'medium' ELSE 'small' END AS score_tier
    FROM sequenced
    WHERE distinct_student_id_in_enrolled_at_to_date_cours >= 4
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(score) OVER w_distinct_student_id_in_enrol, 2) AS distinct_student_id_in_enrolled_at_to_date_cours_2,
        ROUND(PERCENT_RANK() OVER w_grade_status_by_distinct_stu, 3) AS distinct_student_id_in_enrolled_at_to_date_cours_3,
        LEAD(student_id_sum_score) OVER w_enrolled_at_year_score_tier AS next_student_id_sum_score
    FROM enriched
    WINDOW w_distinct_student_id_in_enrol AS (PARTITION BY distinct_student_id_in_enrolled_at_to_date_cours, student_id), w_grade_status_by_distinct_stu AS (PARTITION BY grade_status ORDER BY distinct_student_id_in_enrolled_at_to_date_cours NULLS LAST, id), w_enrolled_at_year_score_tier AS (PARTITION BY YEAR(enrolled_at), score_tier ORDER BY enrolled_at, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        student_id,
        grade_status,
        TO_DATE(enrolled_at) AS enrolled_at_day,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM with_windows
    WHERE HOUR(enrolled_at) BETWEEN 6 AND 12
    GROUP BY 1, 2, 3
)
SELECT
    student_id,
    enrolled_at_day,
    grade_status_list,
    median_score,
    DATEDIFF(DATE '2025-06-30', enrolled_at_day) AS days_since_enrolled_at_day
FROM grouped
ORDER BY days_since_enrolled_at_day DESC

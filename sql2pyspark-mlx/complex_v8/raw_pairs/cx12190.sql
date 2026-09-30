WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at,
        score - LAG(score) OVER (PARTITION BY student_id, grade_status ORDER BY score ASC, id) AS score_change,
        ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY score ASC, id) AS score_seq,
        score - LAG(score) OVER (PARTITION BY student_id, course_id ORDER BY score, id) AS score_change_2,
        MIN(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        ROUND(PERCENT_RANK() OVER w_grade_status_course_id_by_en, 3) AS enrolled_at_percent_rank,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY student_id, course_id ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        LAG(score, 1, 0) OVER w_grade_status_by_score AS prev_score,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY grade_status, course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        SUM(score) OVER (PARTITION BY student_id, TO_DATE(enrolled_at)) AS student_id_enrolled_at_to_date_sum_score,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        MAX(score) OVER (PARTITION BY grade_status, student_id) AS grade_status_student_id_max_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at, id), 3) AS enrolled_at_percent_rank_2
    FROM cleaned
    WINDOW w_grade_status_course_id_by_en AS (PARTITION BY grade_status, course_id ORDER BY enrolled_at), w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score DESC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        enrolled_at,
        score_seq,
        score_change_2,
        min_score_so_far,
        second_score,
        enrolled_at_percent_rank,
        prev_score,
        student_id_enrolled_at_to_date_sum_score,
        max_score_so_far,
        grade_status_student_id_max_score,
        enrolled_at_percent_rank_2,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        INITCAP(TRIM(grade_status)) AS grade_status_clean
    FROM sequenced
),
stats AS (
    -- Summarise per group
    SELECT
        score_seq,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        MAX(enrolled_at) AS last_enrolled_at,
        PERCENTILE_APPROX(enrolled_at_percent_rank, 0.5) AS median_enrolled_at_percent_rank
    FROM scored
    GROUP BY 1, 2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(score_seq, 2) OVER (PARTITION BY score_seq ORDER BY last_enrolled_at, score_seq, enrolled_at_month) AS next2_score_seq,
        ROUND(AVG(score_seq) OVER w_score_seq_last_enrolled_at_t, 2) AS score_seq_last_enrolled_at_to_date_avg_score_seq,
        LAST_VALUE(enrolled_at_month) IGNORE NULLS OVER (PARTITION BY score_seq ORDER BY last_enrolled_at, score_seq, enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_month,
        ROW_NUMBER() OVER (PARTITION BY score_seq ORDER BY last_enrolled_at, score_seq, enrolled_at_month) AS seq_no
    FROM stats
    WINDOW w_score_seq_last_enrolled_at_t AS (PARTITION BY score_seq, TO_DATE(last_enrolled_at))
)
-- Final projection
SELECT
    score_seq,
    enrolled_at_month,
    next2_score_seq,
    score_seq_last_enrolled_at_to_date_avg_score_seq,
    last_known_enrolled_at_month,
    seq_no,
    FLOOR(next2_score_seq / 2) * 2 AS next2_score_seq_bucket
FROM metrics
ORDER BY next2_score_seq DESC, enrolled_at_month, score_seq
LIMIT 5

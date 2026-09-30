WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
),
joined_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_len,
        st.is_international AS student_is_international
    FROM cleaned_enrollments s
    JOIN students st ON st.id = s.student_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_student_id_grade_status_len AS rows_in_student_id_grade_status_len,
        score - LAG(score) OVER (PARTITION BY course_id ORDER BY grade_status_len, id) AS score_change,
        SUM(score) OVER (PARTITION BY student_is_international, grade_status_len ORDER BY grade_status_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status_len, student_is_international ORDER BY enrolled_at DESC NULLS FIRST), 3) AS enrolled_at_percent_rank,
        LAG(score) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS prev_score,
        MAX(score) OVER (PARTITION BY grade_status_len, student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS enrolled_at_rank,
        LEAD(grade_status_len) OVER (PARTITION BY student_is_international, grade_status_len ORDER BY grade_status_len DESC, id) AS next_grade_status_len,
        MIN(score) OVER (PARTITION BY grade_status_len, student_is_international ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        SIZE(COLLECT_SET(course_id) OVER w_grade_status) AS distinct_course_id_in_grade_status,
        MIN(grade_status_len) OVER (PARTITION BY student_id, grade_status_len) AS student_id_grade_status_len_min_grade_status_len,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY grade_status_len, id) AS rn,
        NTILE(10) OVER w_course_id_by_grade_status_le AS grade_status_len_decile
    FROM joined_student
    WINDOW w_student_id_grade_status_len AS (PARTITION BY student_id, grade_status_len), w_grade_status AS (PARTITION BY grade_status), w_course_id_by_grade_status_le AS (PARTITION BY course_id ORDER BY grade_status_len, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        student_is_international,
        score_change,
        enrolled_at_percent_rank,
        max_score_so_far,
        next_grade_status_len,
        distinct_course_id_in_grade_status,
        student_id_grade_status_len_min_grade_status_len,
        first_known_enrolled_at,
        DENSE_RANK() OVER w_all_by_running_score AS running_score_dense_rank,
        MIN(enrolled_at_percent_rank) OVER (PARTITION BY enrolled_at_rank, term) AS enrolled_at_rank_term_min_enrolled_at_percent_ra,
        ROUND(CUME_DIST() OVER (ORDER BY prev_score), 3) AS prev_score_cume_dist,
        ROUND(AVG(next_grade_status_len) OVER (PARTITION BY rn ORDER BY score_change, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS next_grade_status_len_avg_2,
        MAX(min_score_so_far) OVER (PARTITION BY grade_status_len_decile, next_grade_status_len) AS grade_status_len_decile_next_grade_status_len_ma,
        LEAD(max_score_so_far, 3) OVER (ORDER BY student_id_grade_status_len_min_grade_status_len, id) AS next3_max_score_so_far,
        SUM(max_score_so_far) OVER (ORDER BY prev_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_score_so_far,
        ROUND(enrolled_at_percent_rank / NULLIF(SUM(enrolled_at_percent_rank) OVER w_enrolled_at_rank_term, 0), 4) AS enrolled_at_percent_rank_share_of_enrolled_at_ra
    FROM metrics
    WINDOW w_all_by_running_score AS (ORDER BY running_score DESC, id), w_enrolled_at_rank_term AS (PARTITION BY enrolled_at_rank, term)
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(enrolled_at) AS enrolled_at_day,
        CASE WHEN score_change >= 60.0 THEN 'A' WHEN score_change >= -20.0 THEN 'B' ELSE 'C' END AS score_change_tier,
        student_is_international,
        ROUND(AVG(next3_max_score_so_far), 2) AS avg_next3_max_score_so_far,
        MAX(score_change) AS max_score_change
    FROM sequenced
    GROUP BY 1, 2, 3
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_all_by_avg_next3_max_score_s AS avg_next3_max_score_so_far_rank,
        ROUND(avg_next3_max_score_so_far - AVG(avg_next3_max_score_so_far) OVER (PARTITION BY score_change_tier, student_is_international), 2) AS avg_next3_max_score_so_far_vs_score_change_tier_,
        MAX(avg_next3_max_score_so_far) OVER (PARTITION BY score_change_tier, student_is_international) AS score_change_tier_student_is_international_max_a,
        LAG(avg_next3_max_score_so_far, 2) OVER (ORDER BY avg_next3_max_score_so_far, enrolled_at_day, score_change_tier, student_is_international) AS prev2_avg_next3_max_score_so_far
    FROM per_group
    WINDOW w_all_by_avg_next3_max_score_s AS (ORDER BY avg_next3_max_score_so_far DESC NULLS FIRST)
)
-- Final projection
SELECT
    enrolled_at_day,
    score_change_tier,
    student_is_international,
    avg_next3_max_score_so_far,
    avg_next3_max_score_so_far_vs_score_change_tier_,
    score_change_tier_student_is_international_max_a,
    DATE_FORMAT(enrolled_at_day, 'yyyy-MM') AS enrolled_at_day_label
FROM metrics_2
ORDER BY enrolled_at_day, score_change_tier, student_is_international
LIMIT 5

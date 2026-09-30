WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM enrollments
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_bucket,
        c.course_code
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE c.course_code IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(score_bucket - AVG(score_bucket) OVER w_grade_status, 2) AS score_bucket_vs_grade_status_avg,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY grade_status), 0), 4) AS score_bucket_share_of_grade_status,
        RANK() OVER (ORDER BY enrolled_at, id) AS enrolled_at_rank,
        NTH_VALUE(score_bucket, 2) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_bucket,
        RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_rank_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank,
        LAST_VALUE(score_bucket) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_bucket,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        LAG(enrolled_at) OVER w_all_by_enrolled_at AS prev_enrolled_at,
        RANK() OVER w_course_id_by_enrolled_at_tie AS enrolled_at_rank_3,
        ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id) AS seq_no
    FROM enriched_course
    WINDOW w_grade_status AS (PARTITION BY grade_status), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_course_id_by_enrolled_at_tie AS (PARTITION BY course_id ORDER BY enrolled_at DESC)
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        score_bucket,
        course_code,
        score_bucket_share_of_grade_status,
        enrolled_at_rank,
        second_score_bucket,
        last_known_score_bucket,
        prev_enrolled_at,
        seq_no,
        SIZE(COLLECT_SET(grade_status) OVER w_enrolled_at_rank_3_first_enr) AS distinct_grade_status_in_enrolled_at_rank_3_firs,
        SUM(enrolled_at_rank_3) OVER (PARTITION BY enrolled_at_rank_2 ORDER BY enrolled_at_percent_rank DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS enrolled_at_rank_3_sum_3_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrolled_at_rank_2 ORDER BY enrolled_at_percent_rank DESC), 3) AS enrolled_at_percent_rank_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY enrolled_at_rank_2 ORDER BY enrolled_at_percent_rank DESC, id), 3) AS enrolled_at_percent_rank_percent_rank_2,
        MAX(enrolled_at_percent_rank) OVER (PARTITION BY enrolled_at_rank_3, DATE_TRUNC('MONTH', first_enrolled_at)) AS enrolled_at_rank_3_first_enrolled_at_month_max_e
    FROM with_trends
    WINDOW w_enrolled_at_rank_3_first_enr AS (PARTITION BY enrolled_at_rank_3, DATE_TRUNC('MONTH', first_enrolled_at))
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_rank,
        MAX(score_bucket) AS max_score_bucket,
        COUNT(*) AS cnt,
        SUM(CASE WHEN LENGTH(TRIM(course_code)) >= 4 THEN score_bucket ELSE 0 END) AS score_bucket_length_trim_course_code_4
    FROM with_trends_2
    GROUP BY 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(max_score_bucket) OVER (PARTITION BY enrolled_at_rank ORDER BY score_bucket_length_trim_course_code_4 DESC, enrolled_at_rank ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_score_bucket_sum_2,
        cnt - LAG(cnt) OVER (PARTITION BY enrolled_at_rank ORDER BY score_bucket_length_trim_course_code_4 DESC, enrolled_at_rank) AS cnt_change,
        RANK() OVER (PARTITION BY enrolled_at_rank ORDER BY score_bucket_length_trim_course_code_4 DESC, enrolled_at_rank) AS score_bucket_length_trim_course_code_4_rank
    FROM summary
)
-- Final projection
SELECT
    enrolled_at_rank,
    score_bucket_length_trim_course_code_4,
    max_score_bucket_sum_2,
    cnt_change,
    ABS(cnt - 6) AS cnt_distance,
    COALESCE(max_score_bucket, 0) AS max_score_bucket_filled
FROM ranked
WHERE score_bucket_length_trim_course_code_4_rank <= 8
ORDER BY max_score_bucket_sum_2, score_bucket_length_trim_course_code_4 DESC

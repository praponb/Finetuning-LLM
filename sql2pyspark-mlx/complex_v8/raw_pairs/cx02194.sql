WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(grade_status, '#', CAST(id AS STRING)) AS grade_status_ref,
        CASE WHEN score >= 73.7 THEN 'large' WHEN score >= 39.4 THEN 'medium' ELSE 'small' END AS score_tier,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE student_id IS NOT NULL
),
term_stats AS (
    -- Avg score per term
    SELECT term, AVG(score) AS avg_score
    FROM cleaned
    GROUP BY term
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM cleaned s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score > g.avg_score
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status_ref,
        score_tier,
        enrolled_at_year,
        avg_score,
        LENGTH(TRIM(term)) AS term_len,
        ROUND(score_minus_avg * 1.5, 2) AS score_minus_avg_adj,
        CONCAT(grade_status_ref, '#', CAST(course_id AS STRING)) AS grade_status_ref_ref
    FROM vs_group
    WHERE score_minus_avg > -30.0
),
summary AS (
    -- Summarise per group
    SELECT
        term_len,
        score_tier,
        ROUND(SUM(score), 2) AS total_score,
        MIN(enrolled_at_year) AS first_enrolled_at_year,
        MAX(score) AS max_score,
        ROUND(AVG(score_minus_avg_adj), 2) AS avg_score_minus_avg_adj
    FROM annotated
    GROUP BY term_len, score_tier
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        term_len,
        score_tier,
        total_score,
        first_enrolled_at_year,
        max_score,
        LEAD(max_score) OVER (ORDER BY avg_score_minus_avg_adj DESC, term_len, score_tier) AS next_max_score,
        SUM(max_score) OVER (PARTITION BY TO_DATE(first_enrolled_at_year) ORDER BY total_score, term_len, score_tier ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS max_score_sum_2_prior,
        SUM(total_score) OVER (PARTITION BY TO_DATE(first_enrolled_at_year) ORDER BY total_score, term_len, score_tier ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_score_sum_7,
        ROUND(AVG(avg_score_minus_avg_adj) OVER (ORDER BY avg_score_minus_avg_adj DESC, term_len, score_tier ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS avg_score_minus_avg_adj_avg_3_prior,
        ROUND(AVG(term_len) OVER (PARTITION BY term_len, score_tier), 2) AS term_len_score_tier_avg_term_len
    FROM summary
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM windowed
    WHERE max_score > (SELECT MIN(max_score) FROM windowed)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(next_max_score) OVER w_first_enrolled_at_year_month AS first_enrolled_at_year_month_score_tier_max_next,
        ROW_NUMBER() OVER (ORDER BY max_score DESC, term_len, score_tier) AS max_score_seq,
        ROUND(AVG(total_score) OVER (ORDER BY max_score DESC, term_len, score_tier ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS total_score_avg_3,
        COUNT(*) OVER w_first_enrolled_at_year_month AS rows_in_first_enrolled_at_year_month_score_tier,
        total_score_sum_7 - LAG(total_score_sum_7) OVER (ORDER BY max_score DESC, term_len, score_tier) AS total_score_sum_7_change
    FROM screened
    WINDOW w_first_enrolled_at_year_month AS (PARTITION BY MONTH(first_enrolled_at_year), score_tier)
)
-- Final projection
SELECT
    term_len,
    total_score,
    first_enrolled_at_year,
    max_score,
    next_max_score,
    max_score_sum_2_prior,
    total_score_sum_7,
    first_enrolled_at_year_month_score_tier_max_next,
    max_score_seq,
    total_score_avg_3,
    rows_in_first_enrolled_at_year_month_score_tier,
    ROUND(total_score_sum_7_change / NULLIF(term_len, 0), 3) AS total_score_sum_7_change_per_term_len
FROM with_windows
ORDER BY max_score DESC

WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 71.0 THEN 'large' WHEN score >= 32.7 THEN 'medium' ELSE 'small' END AS score_tier,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE student_id <= 3
      AND course_id <= 5
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        LENGTH(TRIM(score_tier)) AS score_tier_len
    FROM valid
    WHERE term_len BETWEEN 6 AND 11
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(score_tier_len) OVER (PARTITION BY term_len ORDER BY term_len DESC, id) AS next_score_tier_len,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        ROUND(score_tier_len - AVG(score_tier_len) OVER w_term, 2) AS score_tier_len_vs_term_avg,
        FIRST_VALUE(score_tier_len) IGNORE NULLS OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_tier_len,
        LAG(enrolled_at, 3) OVER (ORDER BY enrolled_at DESC, id) AS prev3_enrolled_at
    FROM labeled
    WINDOW w_term AS (PARTITION BY term)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_tier,
        term_len,
        grade_status_slug,
        score_tier_len,
        rows_in_term,
        score_tier_len_vs_term_avg,
        first_known_score_tier_len,
        prev3_enrolled_at,
        ROUND(rows_in_term / NULLIF(score_tier_len, 0), 3) AS rows_in_term_per_score_tier_len,
        COALESCE(next_score_tier_len, 0) AS next_score_tier_len_filled,
        COALESCE(score_tier_len, 0) AS score_tier_len_filled
    FROM windowed
    WHERE term_len < 9
),
joined AS (
    -- Join courses
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_slug,
        s.score_tier_len,
        s.rows_in_term,
        s.score_tier_len_vs_term_avg,
        s.prev3_enrolled_at,
        s.rows_in_term_per_score_tier_len,
        s.next_score_tier_len_filled,
        s.score_tier_len_filled,
        c.course_code,
        c.level AS course_level,
        c.dept AS course_dept
    FROM scored s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    course_id,
    grade_status,
    enrolled_at,
    grade_status_slug,
    score_tier_len,
    rows_in_term,
    score_tier_len_vs_term_avg,
    prev3_enrolled_at,
    rows_in_term_per_score_tier_len,
    course_code,
    course_dept,
    DATEDIFF(DATE '2025-06-30', prev3_enrolled_at) AS days_since_prev3_enrolled_at,
    ROUND(score_tier_len / NULLIF(score_tier_len_filled, 0), 3) AS score_tier_len_per_score_tier_len_filled
FROM joined
ORDER BY score_tier_len_vs_term_avg

WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
),
enriched_course AS (
    -- Attach course attributes
    SELECT s.*, c.credits AS course_credits
    FROM valid s
    LEFT JOIN courses c ON c.id = s.course_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        term,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        MAX(score) AS max_score,
        COUNT(*) FILTER (WHERE term = '2025T2') AS n_term_2025t2,
        ROUND(SUM(score_adj) / NULLIF(SUM(score), 0), 3) AS score_adj_to_score_ratio
    FROM enriched_course
    GROUP BY term, DATE_TRUNC('MONTH', enrolled_at)
    HAVING AVG(course_credits) > 2
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE max_score > (SELECT AVG(max_score) FROM rollup_base)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(enrolled_at_month, 'MMM yyyy') AS enrolled_at_month_label,
        DATE_TRUNC('QUARTER', enrolled_at_month) AS enrolled_at_month_quarter,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM without_matches
)
SELECT
    term,
    enrolled_at_month,
    n_term_2025t2,
    score_adj_to_score_ratio,
    enrolled_at_month_label,
    enrolled_at_month_quarter,
    term_or_unknown,
    CASE WHEN score_adj_to_score_ratio >= 5.5 THEN 'A' WHEN score_adj_to_score_ratio >= 2.5 THEN 'B' ELSE 'C' END AS score_adj_to_score_ratio_tier
FROM enriched
WHERE score_adj_to_score_ratio > 4.0
ORDER BY enrolled_at_month_quarter

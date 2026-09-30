WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        ROUND(score * 1.1, 2) AS score_adj_2,
        LOWER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE (score < 80.2 OR enrolled_at < TIMESTAMP '2025-03-02 08:00:00')
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        s.score_adj_2,
        c.credits AS course_credits,
        st.cohort AS student_cohort
    FROM valid_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id AND c.dept IN ('Biz', 'CS', 'Art')
    LEFT JOIN students st ON st.id = s.student_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        term,
        MAX(score_adj_2) AS max_score_adj_2,
        MIN(score_adj_2) AS min_score_adj_2,
        COUNT(*) AS cnt,
        SUM(score_adj_2) FILTER (WHERE term = '2024T2') AS score_adj_2_term_2024t2,
        COUNT(*) FILTER (WHERE term = '2025T2') AS n_term_2025t2
    FROM joined
    WHERE grade_status IN ('INCOMPLETE', 'PASS')
    GROUP BY 1, 2
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM per_group
    WHERE score_adj_2_term_2024t2 < (SELECT MAX(score_adj_2_term_2024t2) FROM per_group)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        enrolled_at_month,
        term,
        max_score_adj_2,
        min_score_adj_2,
        cnt,
        score_adj_2_term_2024t2,
        n_term_2025t2,
        ROW_NUMBER() OVER (PARTITION BY cnt, n_term_2025t2 ORDER BY max_score_adj_2 DESC, enrolled_at_month, term) AS max_score_adj_2_seq,
        MAX(score_adj_2_term_2024t2) OVER (PARTITION BY cnt, n_term_2025t2 ORDER BY max_score_adj_2 DESC, enrolled_at_month, term ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_adj_2_term_2024t2_so_far,
        LAG(enrolled_at_month) OVER (PARTITION BY cnt ORDER BY min_score_adj_2, enrolled_at_month, term) AS prev_enrolled_at_month,
        COUNT(*) OVER w_enrolled_at_month_year_term AS rows_in_enrolled_at_month_year_term
    FROM without_matches
    WINDOW w_enrolled_at_month_year_term AS (PARTITION BY YEAR(enrolled_at_month), term)
)
-- Native SQL UNPIVOT (wide to long)
SELECT enrolled_at_month, term, metric, reading
FROM (
    SELECT enrolled_at_month, term, max_score_adj_2_term_2024t2_so_far, score_adj_2_term_2024t2, min_score_adj_2, max_score_adj_2
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (max_score_adj_2_term_2024t2_so_far, score_adj_2_term_2024t2, min_score_adj_2, max_score_adj_2)
) unp
ORDER BY enrolled_at_month, term, metric

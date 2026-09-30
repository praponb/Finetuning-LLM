WITH scoped AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
grade_status_stats AS (
    -- Avg score per grade_status
    SELECT grade_status, AVG(score) AS avg_score
    FROM scoped
    GROUP BY grade_status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM scoped s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score > g.avg_score
),
with_details_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.score_minus_avg,
        c.course_code
    FROM vs_group s
    LEFT JOIN courses c ON c.id = s.course_id AND c.credits > 2
),
totals AS (
    -- Summarise per group
    SELECT
        term,
        grade_status,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(*) AS cnt
    FROM with_details_course
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        term,
        grade_status,
        term_list,
        cnt,
        ROUND(AVG(cnt) OVER (PARTITION BY cnt, term), 2) AS cnt_term_avg_cnt,
        DENSE_RANK() OVER w_cnt_by_cnt_ties AS cnt_dense_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY cnt, term)) AS distinct_grade_status_in_cnt_term,
        ROUND(cnt - AVG(cnt) OVER (PARTITION BY cnt, term), 2) AS cnt_vs_cnt_term_avg
    FROM totals
    WINDOW w_cnt_by_cnt_ties AS (PARTITION BY cnt ORDER BY cnt)
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_grade_status_in_cnt_term,
    COALESCE(term, 'ALL') AS term,
    cnt_dense_rank,
    GROUPING_ID() AS gid,
    MIN(cnt_dense_rank) AS min_cnt_dense_rank,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list_2,
    COUNT(*) AS cnt_2
FROM sequenced
GROUP BY GROUPING SETS ((distinct_grade_status_in_cnt_term, term, cnt_dense_rank), (distinct_grade_status_in_cnt_term), ())

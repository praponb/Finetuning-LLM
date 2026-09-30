WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        ROUND(score * 0.25, 2) AS score_adj,
        ABS(score - 57.8) AS score_distance,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE grade_status NOT IN ('WITHDRAWN', 'FAIL')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM filtered
),
unique_rows AS (
    -- Keep the latest row per term, grade_status
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_adj,
        score_distance,
        enrolled_at_year,
        grade_status_len,
        grade_status_term_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, grade_status ORDER BY grade_status_len DESC, id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(score_distance) OVER (PARTITION BY course_id ORDER BY score, id) AS prev_score_distance,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY grade_status, grade_status_len), 0), 4) AS score_share_of_grade_status_grade_status_len
    FROM unique_rows
),
per_group AS (
    -- Group-level totals
    SELECT
        course_id,
        term,
        enrolled_at_year,
        ROUND(100 * COUNT(CASE WHEN term = '2024T1' THEN 1 END) / COUNT(*), 1) AS pct_term_2024t1,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        SUM(grade_status_len) AS total_grade_status_len,
        ROUND(AVG(grade_status_len), 2) AS avg_grade_status_len
    FROM with_trends
    WHERE course_id IS NOT NULL
    GROUP BY 1, 2, 3
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(avg_grade_status_len * 0.25, 2) AS avg_grade_status_len_adj,
        ABS(enrolled_at_year - 2025) AS enrolled_at_year_distance,
        ROUND(avg_grade_status_len * 1.07, 2) AS avg_grade_status_len_adj_2
    FROM per_group
)
SELECT
    course_id,
    term,
    enrolled_at_year,
    grade_status_list,
    total_grade_status_len,
    avg_grade_status_len,
    enrolled_at_year_distance,
    avg_grade_status_len_adj_2,
    LENGTH(TRIM(term)) AS term_len
FROM tagged

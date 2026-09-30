WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at_2
    FROM enrollments
    WHERE grade_status NOT IN ('WITHDRAWN', 'FAIL')
),
grouped AS (
    -- Summarise per group
    SELECT
        term,
        COUNT(*) FILTER (WHERE term IN ('2024T2', '2024T1', '2025T2')) AS n_term_in_2024t2_2024t1_2025t2,
        SUM(days_since_enrolled_at_2) FILTER (WHERE term = '2024T2') AS days_since_enrolled_at_2_term_2024t2,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN term NOT IN ('2025T1', '2025T2') THEN 1 END) / COUNT(*), 1) AS pct_term_not_in_2025t1_2025t2,
        ROUND(100 * COUNT(CASE WHEN term = '2024T1' THEN 1 END) / COUNT(*), 1) AS pct_term_2024t1
    FROM recent_enrollments
    GROUP BY term
    HAVING AVG(score) > 10.1
),
annotated AS (
    -- Add calculated columns
    SELECT
        term,
        n_term_in_2024t2_2024t1_2025t2,
        days_since_enrolled_at_2_term_2024t2,
        distinct_ids,
        pct_term_not_in_2025t1_2025t2,
        pct_term_2024t1,
        LENGTH(TRIM(term)) AS term_len,
        pct_term_2024t1 + distinct_ids AS pct_term_2024t1_plus_distinct_ids,
        CONCAT(term, '#', CAST(distinct_ids AS STRING)) AS term_ref
    FROM grouped
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(distinct_ids) OVER w_n_term_in_2024t2_2024t1_2025 AS n_term_in_2024t2_2024t1_2025t2_distinct_ids_min_,
        MAX(distinct_ids) OVER (ORDER BY distinct_ids, term ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_ids_max_3,
        ROUND(CUME_DIST() OVER (ORDER BY distinct_ids, term), 3) AS distinct_ids_cume_dist,
        MAX(term_len) OVER (ORDER BY distinct_ids, term ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS term_len_max_3
    FROM annotated
    WINDOW w_n_term_in_2024t2_2024t1_2025 AS (PARTITION BY n_term_in_2024t2_2024t1_2025t2, distinct_ids)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE term_len < (SELECT MAX(term_len) FROM sequenced)
)
SELECT
    term,
    n_term_in_2024t2_2024t1_2025t2,
    days_since_enrolled_at_2_term_2024t2,
    distinct_ids,
    pct_term_not_in_2025t1_2025t2,
    term_len,
    pct_term_2024t1_plus_distinct_ids,
    term_ref,
    n_term_in_2024t2_2024t1_2025t2_distinct_ids_min_,
    distinct_ids_max_3,
    distinct_ids_cume_dist,
    term_len_max_3,
    LENGTH(TRIM(term_ref)) AS term_ref_len
FROM with_activity
ORDER BY distinct_ids_max_3, term
LIMIT 5

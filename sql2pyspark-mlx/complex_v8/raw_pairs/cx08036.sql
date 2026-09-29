WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE score <= 85.2
      AND term <> '2025T2'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY grade_status, term ORDER BY enrolled_at, id) AS enrolled_at_quintile,
        COUNT(*) OVER (PARTITION BY TO_DATE(enrolled_at)) AS rows_in_enrolled_at_to_date,
        MIN(enrolled_at_dayofweek) OVER (PARTITION BY YEAR(enrolled_at), grade_status ORDER BY enrolled_at_dayofweek NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_enrolled_at_dayofweek_so_far,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY YEAR(enrolled_at), grade_status ORDER BY enrolled_at_dayofweek NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(PERCENT_RANK() OVER w_enrolled_at_year_grade_statu_3, 3) AS enrolled_at_dayofweek_percent_rank,
        DENSE_RANK() OVER w_all_by_enrolled_at_dayofweek AS enrolled_at_dayofweek_dense_rank,
        COUNT(*) OVER (ORDER BY enrolled_at_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(enrolled_at_dayofweek) IGNORE NULLS OVER (ORDER BY enrolled_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at_dayofweek,
        ROUND(AVG(enrolled_at_dayofweek) OVER (ORDER BY enrolled_at_dayofweek ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrolled_at_dayofweek_avg_2_prior
    FROM recent
    WINDOW w_enrolled_at_year_grade_statu_3 AS (PARTITION BY YEAR(enrolled_at), grade_status ORDER BY enrolled_at_dayofweek NULLS LAST, id), w_all_by_enrolled_at_dayofweek AS (ORDER BY enrolled_at_dayofweek, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN grade_status IN ('WITHDRAWN', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        CONCAT(grade_status, '#', CAST(running_count AS STRING)) AS grade_status_ref
    FROM metrics
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at_dayofweek,
        grade_status_term_label,
        enrolled_at_quintile,
        min_enrolled_at_dayofweek_so_far,
        second_enrolled_at,
        enrolled_at_dayofweek_dense_rank,
        enrolled_at_dayofweek_avg_2_prior,
        grade_status_group,
        grade_status_ref,
        COUNT(*) OVER w_enrolled_at_dayofweek AS rows_in_enrolled_at_dayofweek,
        COUNT(*) OVER (PARTITION BY last_known_enrolled_at_dayofweek, enrolled_at_dayofweek) AS rows_in_last_known_enrolled_at_dayofweek_enrolle,
        SUM(enrolled_at_quintile) OVER (PARTITION BY student_id ORDER BY enrolled_at_dayofweek_avg_2_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_enrolled_at_quintile,
        NTH_VALUE(running_count, 2) OVER (PARTITION BY grade_status_group, TO_DATE(second_enrolled_at) ORDER BY min_enrolled_at_dayofweek_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_count
    FROM annotated
    WINDOW w_enrolled_at_dayofweek AS (PARTITION BY enrolled_at_dayofweek)
)
SELECT
    id,
    course_id,
    term,
    grade_status,
    grade_status_term_label,
    enrolled_at_quintile,
    min_enrolled_at_dayofweek_so_far,
    enrolled_at_dayofweek_dense_rank,
    grade_status_ref,
    rows_in_last_known_enrolled_at_dayofweek_enrolle,
    running_enrolled_at_quintile
FROM sequenced
ORDER BY running_enrolled_at_quintile, enrolled_at_quintile DESC

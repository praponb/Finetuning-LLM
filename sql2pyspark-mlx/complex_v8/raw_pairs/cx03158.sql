WITH prepared_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        major,
        gpa,
        COALESCE(gpa, 0.0) AS gpa_filled
    FROM students
    WHERE major NOT IN ('CS', 'Math')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER w_major_by_gpa_filled AS gpa_filled_quartile,
        SUM(gpa_filled) OVER (ORDER BY gpa_filled NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS gpa_filled_sum_6_prior
    FROM prepared_students
    WINDOW w_major_by_gpa_filled AS (PARTITION BY major ORDER BY gpa_filled DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        gpa_filled_quartile,
        major,
        ROUND(AVG(gpa_filled), 2) AS avg_gpa_filled,
        ARRAY_SORT(COLLECT_SET(student_name)) AS student_name_list
    FROM metrics
    GROUP BY gpa_filled_quartile, major
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(avg_gpa_filled) IGNORE NULLS OVER (PARTITION BY major, gpa_filled_quartile ORDER BY avg_gpa_filled NULLS LAST, gpa_filled_quartile, major ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_gpa_filled,
        ROUND(CUME_DIST() OVER w_major_gpa_filled_quartile_by_2, 3) AS avg_gpa_filled_cume_dist,
        RANK() OVER (PARTITION BY major, gpa_filled_quartile ORDER BY avg_gpa_filled NULLS LAST) AS avg_gpa_filled_rank,
        MAX(avg_gpa_filled) OVER w_major AS major_max_avg_gpa_filled,
        ROUND(AVG(gpa_filled_quartile) OVER (PARTITION BY major, gpa_filled_quartile ORDER BY avg_gpa_filled NULLS LAST, gpa_filled_quartile, major ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS gpa_filled_quartile_avg_4,
        NTH_VALUE(gpa_filled_quartile, 2) OVER (PARTITION BY major, gpa_filled_quartile ORDER BY avg_gpa_filled NULLS LAST, gpa_filled_quartile, major ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_filled_quartile
    FROM grouped
    WINDOW w_major_gpa_filled_quartile_by_2 AS (PARTITION BY major, gpa_filled_quartile ORDER BY avg_gpa_filled NULLS LAST, gpa_filled_quartile, major), w_major AS (PARTITION BY major)
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics_2
    WHERE avg_gpa_filled_rank > (SELECT MIN(avg_gpa_filled_rank) FROM metrics_2)
),
per_label AS (
    -- One row per student_name_list_item
    SELECT
        s.gpa_filled_quartile,
        s.major,
        s.avg_gpa_filled,
        s.last_known_avg_gpa_filled,
        s.avg_gpa_filled_cume_dist,
        s.avg_gpa_filled_rank,
        s.major_max_avg_gpa_filled,
        s.gpa_filled_quartile_avg_4,
        s.second_gpa_filled_quartile,
        t.student_name_list_item
    FROM without_matches s
    LATERAL VIEW OUTER EXPLODE(s.student_name_list) t AS student_name_list_item
)
SELECT
    gpa_filled_quartile,
    avg_gpa_filled,
    last_known_avg_gpa_filled,
    avg_gpa_filled_cume_dist,
    major_max_avg_gpa_filled,
    gpa_filled_quartile_avg_4,
    second_gpa_filled_quartile,
    student_name_list_item,
    ROUND(avg_gpa_filled * 1.5, 2) AS avg_gpa_filled_adj,
    ROUND(avg_gpa_filled * 1.07, 2) AS avg_gpa_filled_adj_2
FROM per_label
WHERE avg_gpa_filled_rank <= 9
ORDER BY major_max_avg_gpa_filled DESC, second_gpa_filled_quartile DESC

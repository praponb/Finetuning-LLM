WITH scoped AS (
    -- Scope the students rows
    SELECT
        id,
        major,
        enrolled_on,
        is_international,
        CASE WHEN cohort IN ('C2024', 'C2021') THEN 'group_a' WHEN cohort = 'C2022' THEN 'group_b' ELSE 'other' END AS cohort_group,
        CASE WHEN gpa >= 3.2 THEN 'HIGH' WHEN gpa >= 2.7 THEN 'MEDIUM' ELSE 'LOW' END AS gpa_tier,
        COALESCE(major, 'UNKNOWN') AS major_or_unknown
    FROM students
    WHERE major IN ('CS', 'Math')
      AND UPPER(TRIM(student_name)) LIKE 'L%'
),
summary AS (
    -- Summarise per group
    SELECT
        gpa_tier,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE major <> 'Biz') AS n_major_biz,
        ARRAY_SORT(COLLECT_SET(major)) AS major_list
    FROM scoped
    GROUP BY gpa_tier
),
exploded AS (
    -- One row per major_list_item
    SELECT
        s.gpa_tier,
        s.row_count,
        s.n_major_biz,
        t.major_list_item_pos,
        t.major_list_item
    FROM summary s
    LATERAL VIEW POSEXPLODE(s.major_list) t AS major_list_item_pos, major_list_item
),
ranked AS (
    -- Previous / next values per key
    SELECT
        gpa_tier,
        n_major_biz,
        major_list_item_pos,
        major_list_item,
        SUM(row_count) OVER (PARTITION BY major_list_item_pos, gpa_tier) AS major_list_item_pos_gpa_tier_sum_row_count,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY major_list_item_pos, gpa_tier), 2) AS row_count_vs_major_list_item_pos_gpa_tier_avg,
        SIZE(COLLECT_SET(major_list_item) OVER w_major_list_item_pos_gpa_tier) AS distinct_major_list_item_in_major_list_item_pos_,
        NTILE(5) OVER w_gpa_tier_major_list_item_pos AS n_major_biz_quintile,
        FIRST_VALUE(n_major_biz) OVER (PARTITION BY gpa_tier, major_list_item_pos ORDER BY n_major_biz, gpa_tier, major_list_item_pos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_major_biz
    FROM exploded
    WINDOW w_major_list_item_pos_gpa_tier AS (PARTITION BY major_list_item_pos, gpa_tier), w_gpa_tier_major_list_item_pos AS (PARTITION BY gpa_tier, major_list_item_pos ORDER BY n_major_biz, gpa_tier, major_list_item_pos)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(major_list_item_pos - 2) AS major_list_item_pos_distance,
        FLOOR(n_major_biz_quintile / 2) * 2 AS n_major_biz_quintile_bucket,
        ABS(n_major_biz - 4) AS n_major_biz_distance
    FROM ranked
),
annotated AS (
    -- Derive helper columns
    SELECT *, COALESCE(major_list_item_pos, 0) AS major_list_item_pos_filled
    FROM derived
),
major_list_item_stats AS (
    -- Median n_major_biz per major_list_item
    SELECT major_list_item, PERCENTILE_APPROX(n_major_biz, 0.5) AS median_n_major_biz
    FROM annotated
    GROUP BY major_list_item
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_n_major_biz,
        ROUND(s.n_major_biz - g.median_n_major_biz, 2) AS n_major_biz_minus_median
    FROM annotated s
    JOIN major_list_item_stats g ON g.major_list_item = s.major_list_item
    WHERE s.n_major_biz >= g.median_n_major_biz
)
SELECT *
FROM (
    SELECT n_major_biz_distance, n_major_biz_quintile, gpa_tier, median_n_major_biz
    FROM vs_group
) src
PIVOT (
    COUNT(median_n_major_biz)
    FOR gpa_tier IN ('MEDIUM', 'HIGH', 'LOW')
)
ORDER BY n_major_biz_distance, n_major_biz_quintile

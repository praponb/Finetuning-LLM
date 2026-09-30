WITH cleaned_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        LENGTH(TRIM(dept)) AS dept_len,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE credits < 3
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(credits_filled, 2) OVER (PARTITION BY credits, dept_len ORDER BY credits DESC, id) AS prev2_credits_filled,
        MAX(dept_len) OVER (PARTITION BY credits, dept_len ORDER BY credits DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS dept_len_max_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY credits, dept_len ORDER BY credits DESC), 3) AS credits_cume_dist,
        ROUND(dept_len / NULLIF(SUM(dept_len) OVER w_dept_credits_filled, 0), 4) AS dept_len_share_of_dept_credits_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY dept, credits ORDER BY credits_filled), 3) AS credits_filled_cume_dist,
        MIN(credits_filled) OVER (PARTITION BY dept, credits ORDER BY credits_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_credits_filled_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY dept, credits ORDER BY credits_filled, id), 3) AS credits_filled_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY credits_filled, dept_len ORDER BY dept_len, id), 3) AS dept_len_cume_dist,
        RANK() OVER w_dept_credits_by_credits_fill AS credits_filled_rank,
        SUM(credits) OVER (PARTITION BY dept, credits ORDER BY credits_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS credits_sum_7
    FROM cleaned_courses
    WINDOW w_dept_credits_filled AS (PARTITION BY dept, credits_filled), w_dept_credits_by_credits_fill AS (PARTITION BY dept, credits ORDER BY credits_filled)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(credits_sum_7, 2) OVER (PARTITION BY credits_filled, credits_filled_rank ORDER BY min_credits_filled_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits_sum_7,
        ROUND(dept_len_cume_dist - AVG(dept_len_cume_dist) OVER w_credits_sum_7, 2) AS dept_len_cume_dist_vs_credits_sum_7_avg,
        SUM(prev2_credits_filled) OVER (ORDER BY credits_sum_7, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev2_credits_filled_sum_1_prior,
        SUM(dept_len_max_3_prior) OVER w_credits_sum_7 AS credits_sum_7_sum_dept_len_max_3_prior
    FROM sequenced
    WINDOW w_credits_sum_7 AS (PARTITION BY credits_sum_7)
)
SELECT
    id,
    course_code,
    dept,
    credits,
    credits_filled,
    prev2_credits_filled,
    dept_len_max_3_prior,
    credits_cume_dist,
    dept_len_share_of_dept_credits_filled,
    min_credits_filled_so_far,
    credits_filled_percent_rank,
    second_credits_sum_7,
    prev2_credits_filled_sum_1_prior,
    credits_sum_7_sum_dept_len_max_3_prior,
    LEAST(dept_len_cume_dist, credits_sum_7) AS least_dept_len_cume_dist_credits_sum_7
FROM ranked

WITH cleaned AS (
    -- Filter listeners
    SELECT
        id,
        username,
        country,
        signup_date,
        interests
    FROM listeners
    WHERE LOWER(username) LIKE '%an%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        interests,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS signup_date_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY country ORDER BY signup_date DESC, id), 3) AS signup_date_percent_rank
    FROM cleaned
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE signup_date_percent_rank < (SELECT MAX(signup_date_percent_rank) FROM sequenced)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE signup_date_dense_rank > (SELECT AVG(signup_date_dense_rank) FROM qualified)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        signup_date_dense_rank,
        ROUND(SUM(signup_date_dense_rank) / NULLIF(SUM(signup_date_percent_rank), 0), 3) AS signup_date_dense_rank_to_signup_date_percent_ra,
        COUNT(*) AS cnt,
        SUM(signup_date_percent_rank) AS total_signup_date_percent_rank
    FROM without_matches
    GROUP BY signup_date_dense_rank
    HAVING COUNT(*) >= 2
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(signup_date_dense_rank - AVG(signup_date_dense_rank) OVER (PARTITION BY cnt), 2) AS signup_date_dense_rank_vs_cnt_avg,
        LEAD(cnt, 1, 0) OVER (PARTITION BY cnt, signup_date_dense_rank ORDER BY total_signup_date_percent_rank, signup_date_dense_rank) AS next_cnt
    FROM summary
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(next_cnt, cnt) AS greatest_next_cnt_cnt,
        COALESCE(cnt, 0) AS cnt_filled
    FROM sequenced_2
)
SELECT
    signup_date_dense_rank,
    cnt,
    signup_date_dense_rank_vs_cnt_avg,
    next_cnt,
    COALESCE(greatest_next_cnt_cnt, 0) AS greatest_next_cnt_cnt_filled
FROM derived
WHERE greatest_next_cnt_cnt > 5

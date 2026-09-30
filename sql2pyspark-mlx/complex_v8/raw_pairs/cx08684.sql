WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        MONTH(joined_on) AS joined_on_month
    FROM policyholders
    WHERE risk_score <= 0.8
      AND LENGTH(TRIM(holder_name)) >= 7
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY joined_on_month, MONTH(joined_on) ORDER BY joined_on), 3) AS joined_on_percent_rank,
        NTILE(10) OVER (ORDER BY joined_on_month, id) AS joined_on_month_decile,
        LEAD(risk_score) OVER (PARTITION BY joined_on_month, MONTH(joined_on) ORDER BY joined_on, id) AS next_risk_score,
        NTILE(5) OVER w_all_by_risk_score AS risk_score_quintile,
        LAG(risk_score) OVER (PARTITION BY segment ORDER BY risk_score NULLS LAST, id) AS prev_risk_score,
        MAX(risk_score) OVER w_segment_joined_on_month AS segment_joined_on_month_max_risk_score,
        ROUND(AVG(joined_on_month) OVER w_segment, 2) AS segment_avg_joined_on_month,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER w_segment, 0), 4) AS risk_score_share_of_segment,
        DENSE_RANK() OVER (PARTITION BY joined_on_month, MONTH(joined_on) ORDER BY joined_on) AS joined_on_dense_rank,
        DENSE_RANK() OVER (ORDER BY risk_score, id) AS risk_score_dense_rank,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (ORDER BY joined_on_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on,
        SUM(risk_score) OVER (PARTITION BY segment) AS segment_sum_risk_score
    FROM valid
    WINDOW w_all_by_risk_score AS (ORDER BY risk_score, id), w_segment_joined_on_month AS (PARTITION BY segment, joined_on_month), w_segment AS (PARTITION BY segment)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE risk_score_dense_rank <= 5
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_name,
        region,
        segment,
        next_risk_score,
        risk_score_quintile,
        segment_joined_on_month_max_risk_score,
        segment_avg_joined_on_month,
        risk_score_share_of_segment,
        joined_on_dense_rank,
        risk_score_dense_rank,
        LEAD(joined_on_month, 2) OVER (PARTITION BY joined_on_dense_rank ORDER BY first_known_joined_on DESC NULLS FIRST, id) AS next2_joined_on_month,
        joined_on_month - LAG(joined_on_month) OVER (PARTITION BY joined_on_dense_rank ORDER BY first_known_joined_on DESC NULLS FIRST, id) AS joined_on_month_change,
        DENSE_RANK() OVER (PARTITION BY joined_on_dense_rank ORDER BY risk_score) AS risk_score_dense_rank_2,
        ROUND(risk_score_dense_rank - AVG(risk_score_dense_rank) OVER w_joined_on_month_decile, 2) AS risk_score_dense_rank_vs_joined_on_month_decile_,
        ROW_NUMBER() OVER (PARTITION BY joined_on_dense_rank ORDER BY first_known_joined_on DESC NULLS FIRST, id) AS first_known_joined_on_seq
    FROM top_per_group
    WINDOW w_joined_on_month_decile AS (PARTITION BY joined_on_month_decile)
)
SELECT
    id,
    region,
    segment,
    risk_score_quintile,
    segment_joined_on_month_max_risk_score,
    segment_avg_joined_on_month,
    risk_score_share_of_segment,
    joined_on_dense_rank,
    risk_score_dense_rank,
    joined_on_month_change,
    risk_score_dense_rank_2,
    first_known_joined_on_seq
FROM sequenced
WHERE holder_name IS NOT NULL
ORDER BY risk_score_dense_rank_2 DESC, joined_on_dense_rank

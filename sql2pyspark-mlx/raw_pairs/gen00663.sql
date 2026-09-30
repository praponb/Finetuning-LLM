WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE channel IS NOT DISTINCT FROM 'partner'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
ranked_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct) AS latest_rank_2
        FROM ranked
    ) ranked
    WHERE latest_rank_2 = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        latest_rank,
        latest_rank_2,
        SUM(discount_pct_int) OVER (PARTITION BY promo_code) AS promo_code_sum_discount_pct_int,
        AVG(discount_pct_int) OVER (PARTITION BY promo_code ORDER BY latest_rank_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_int_avg_6,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id) AS event_seq,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY YEAR(starts_on), promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM ranked_promotions
    WHERE promo_code != 'WELCOME5'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        latest_rank_2,
        promo_code_sum_discount_pct_int,
        first_known_channel,
        RANK() OVER (PARTITION BY promo_code ORDER BY event_seq DESC) AS event_seq_rank,
        LAG(discount_pct_int_avg_6, 1) OVER (PARTITION BY promo_code, channel ORDER BY latest_rank, id) AS prev_discount_pct_int_avg_6,
        ROW_NUMBER() OVER (ORDER BY discount_pct_int_avg_6, id) AS seq_no
    FROM metrics
),
scored_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        discount_pct,
        latest_rank_2,
        prev_discount_pct_int_avg_6,
        COUNT(prev_discount_pct_int_avg_6) OVER (PARTITION BY promo_code, channel ORDER BY event_seq_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_prev_discount_pct_int_avg_6,
        SUM(prev_discount_pct_int_avg_6) OVER (PARTITION BY promo_code, channel ORDER BY event_seq_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_prev_discount_pct_int_avg_6,
        AVG(prev_discount_pct_int_avg_6) OVER (PARTITION BY promo_code, channel ORDER BY event_seq_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_prev_discount_pct_int_avg_6
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_code,
        last3_avg_prev_discount_pct_int_avg_6,
        last3_sum_prev_discount_pct_int_avg_6,
        discount_pct
    FROM scored_promotions
) src
UNPIVOT (
    metric_value FOR measure IN (last3_avg_prev_discount_pct_int_avg_6, last3_sum_prev_discount_pct_int_avg_6, discount_pct)
) long_rows;

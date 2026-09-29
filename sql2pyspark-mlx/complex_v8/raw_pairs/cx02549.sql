WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled,
        CONCAT_WS(': ', product, status) AS product_status_label
    FROM policies
    WHERE premium < 3817.5
      AND holder_id >= 18
),
scored AS (
    -- Derive helper columns
    SELECT *, ABS(premium - 2096.6) AS premium_distance
    FROM scoped_policys
    WHERE product NOT IN ('AUTO', 'TRAVEL')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium_filled - AVG(premium_filled) OVER w_holder_id, 2) AS premium_filled_vs_holder_id_avg,
        MAX(premium_filled) OVER (PARTITION BY holder_id, MONTH(end_date) ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_filled_so_far,
        COUNT(*) OVER (PARTITION BY holder_id) AS rows_in_holder_id,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        SUM(premium_distance) OVER (PARTITION BY product ORDER BY end_date DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_distance_sum_1_prior
    FROM scored
    WINDOW w_holder_id AS (PARTITION BY holder_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        end_date,
        premium_distance,
        premium_filled_vs_holder_id_avg,
        max_premium_filled_so_far,
        rows_in_holder_id,
        premium_vs_holder_id_avg,
        premium_distance_sum_1_prior,
        ROUND(rows_in_holder_id - AVG(rows_in_holder_id) OVER w_product_holder_id, 2) AS rows_in_holder_id_vs_product_holder_id_avg,
        LAG(premium) OVER w_holder_id_product_by_premium AS prev_premium,
        MIN(premium_distance) OVER (PARTITION BY product, holder_id) AS product_holder_id_min_premium_distance
    FROM metrics
    WINDOW w_product_holder_id AS (PARTITION BY product, holder_id), w_holder_id_product_by_premium AS (PARTITION BY holder_id, product ORDER BY premium_distance ASC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, product, premium_vs_holder_id_avg
    FROM sequenced
) src
PIVOT (
    COUNT(premium_vs_holder_id_avg)
    FOR product IN ('LIFE', 'HOME', 'TRAVEL', 'AUTO')
)
ORDER BY end_date

WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        YEAR(end_date) AS end_date_year,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        LAST_DAY(start_date) AS start_date_month_end
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product = 'LIFE'
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.end_date_year,
        s.product_or_unknown,
        s.start_date_month_end,
        p.joined_on AS holder_joined_on
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
tagged AS (
    -- Add calculated columns
    SELECT *, ABS(end_date_year - 2025) AS end_date_year_distance
    FROM joined
    WHERE end_date IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_holder_id_holder_joined_on_m, 3) AS start_date_month_end_cume_dist,
        LAG(holder_joined_on, 2) OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY start_date_month_end DESC NULLS FIRST, id) AS prev2_holder_joined_on,
        MAX(end_date_year) OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY start_date_month_end DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS end_date_year_max_2,
        ROUND(AVG(end_date_year) OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY start_date_month_end DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS end_date_year_avg_2_prior,
        LAG(end_date_year_distance, 3) OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY start_date_month_end DESC NULLS FIRST, id) AS prev3_end_date_year_distance,
        LEAD(end_date_year_distance, 1, 0) OVER (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY start_date_month_end DESC NULLS FIRST, id) AS next_end_date_year_distance
    FROM tagged
    WINDOW w_holder_id_holder_joined_on_m AS (PARTITION BY holder_id, MONTH(holder_joined_on) ORDER BY start_date_month_end DESC NULLS FIRST)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        product_or_unknown,
        holder_joined_on,
        end_date_year_distance,
        end_date_year_max_2,
        end_date_year_avg_2_prior,
        next_end_date_year_distance,
        SIZE(COLLECT_SET(product_or_unknown) OVER w_end_date_year_start_date_mon) AS distinct_product_or_unknown_in_end_date_year_sta,
        ROW_NUMBER() OVER (PARTITION BY product, product_or_unknown ORDER BY prev2_holder_joined_on DESC, id) AS rn,
        MIN(prev3_end_date_year_distance) OVER (PARTITION BY YEAR(end_date), MONTH(start_date)) AS end_date_year_start_date_month_min_prev3_end_dat
    FROM metrics
    WINDOW w_end_date_year_start_date_mon AS (PARTITION BY YEAR(end_date), MONTH(start_date))
)
SELECT *
FROM (
    SELECT holder_id, rn, product_or_unknown, end_date_year_start_date_month_min_prev3_end_dat
    FROM with_trends
) src
PIVOT (
    MAX(end_date_year_start_date_month_min_prev3_end_dat)
    FOR product_or_unknown IN ('TRAVEL', 'AUTO', 'HEALTH', 'LIFE')
)
ORDER BY holder_id, rn

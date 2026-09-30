WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ABS(premium - 2945.9) AS premium_distance,
        ABS(premium - 3532.7) AS premium_distance_2,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 2033.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
),
enriched AS (
    -- Add calculated columns
    SELECT *, ROUND(premium_distance_2 * 0.9, 2) AS premium_distance_2_adj
    FROM cleaned_policys
),
holder_id_stats AS (
    -- Median premium per holder_id
    SELECT holder_id, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM enriched
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM enriched s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium >= g.median_premium
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(premium) OVER (PARTITION BY MONTH(start_date)) AS start_date_month_max_premium,
        RANK() OVER w_product_holder_id_by_premium AS premium_distance_2_adj_rank,
        ROUND(median_premium - AVG(median_premium) OVER (PARTITION BY MONTH(start_date)), 2) AS median_premium_vs_start_date_month_avg,
        COUNT(*) OVER (PARTITION BY MONTH(start_date)) AS rows_in_start_date_month
    FROM vs_group
    WINDOW w_product_holder_id_by_premium AS (PARTITION BY product, holder_id ORDER BY premium_distance_2_adj ASC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, premium_distance_2_adj_rank, rows_in_start_date_month
    FROM ranked
) src
UNPIVOT (
    amount FOR measure IN (premium_distance_2_adj_rank, rows_in_start_date_month)
) unp
ORDER BY id, measure

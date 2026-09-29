WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date
    FROM policies
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        product,
        ROUND(SUM(premium), 2) AS total_premium,
        COUNT(*) AS cnt,
        MIN(premium) AS min_premium
    FROM filtered_policys
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), product
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_product_cnt_by_cnt_ties, 3) AS cnt_percent_rank,
        NTILE(5) OVER (PARTITION BY cnt ORDER BY total_premium DESC, start_date_ym, product) AS total_premium_quintile,
        min_premium - LAG(min_premium) OVER w_cnt_product_by_total_premium AS min_premium_change
    FROM grouped
    WINDOW w_product_cnt_by_cnt_ties AS (PARTITION BY product, cnt ORDER BY cnt ASC), w_cnt_product_by_total_premium AS (PARTITION BY cnt, product ORDER BY total_premium ASC, start_date_ym, product)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        start_date_ym,
        product,
        total_premium,
        cnt,
        min_premium,
        cnt_percent_rank,
        min_premium_change,
        ROUND(CUME_DIST() OVER (PARTITION BY total_premium_quintile ORDER BY total_premium_quintile DESC), 3) AS total_premium_quintile_cume_dist,
        LAG(min_premium, 3) OVER (PARTITION BY total_premium_quintile ORDER BY total_premium_quintile DESC, start_date_ym, product) AS prev3_min_premium
    FROM ranked
)
-- Native SQL UNPIVOT (wide to long)
SELECT start_date_ym, product, field, reading
FROM (
    SELECT start_date_ym, product, total_premium_quintile_cume_dist, total_premium
    FROM with_trends
) src
UNPIVOT (
    reading FOR field IN (total_premium_quintile_cume_dist, total_premium)
) unp

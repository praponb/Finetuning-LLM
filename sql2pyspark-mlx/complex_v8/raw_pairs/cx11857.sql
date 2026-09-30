WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ADD_MONTHS(end_date, 3) AS end_date_plus_3m,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter
    FROM policies
    WHERE premium BETWEEN 1541.8 AND 4688.7
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        end_date_plus_3m,
        end_date_label,
        end_date_quarter,
        ROUND(CUME_DIST() OVER (ORDER BY premium NULLS LAST), 3) AS premium_cume_dist,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, holder_id ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date
    FROM prepared
),
product_stats AS (
    -- Avg premium_cume_dist per product
    SELECT product, AVG(premium_cume_dist) AS avg_premium_cume_dist
    FROM with_windows
    GROUP BY product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_cume_dist,
        ROUND(s.premium_cume_dist - g.avg_premium_cume_dist, 2) AS premium_cume_dist_minus_avg
    FROM with_windows s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium_cume_dist > g.avg_premium_cume_dist
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER w_first_start_date_year_by_pre AS premium_cume_dist_quartile,
        DATEDIFF(end_date_plus_3m, LAG(end_date_plus_3m) OVER (ORDER BY end_date, id)) AS days_since_prev_end_date_plus_3m,
        ROUND(premium_cume_dist_minus_avg - AVG(premium_cume_dist_minus_avg) OVER (PARTITION BY product, holder_id), 2) AS premium_cume_dist_minus_avg_vs_product_holder_id,
        ROUND(AVG(avg_premium_cume_dist) OVER (ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS avg_premium_cume_dist_avg_3
    FROM vs_group
    WINDOW w_first_start_date_year_by_pre AS (PARTITION BY YEAR(first_start_date) ORDER BY premium_cume_dist DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, avg_premium_cume_dist_avg_3, avg_premium_cume_dist
    FROM sequenced
) src
UNPIVOT (
    metric_value FOR metric_name IN (avg_premium_cume_dist_avg_3, avg_premium_cume_dist)
) unp

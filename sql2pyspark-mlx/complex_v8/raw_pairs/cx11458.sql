WITH cleaned AS (
    -- Filter sites
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ROUND(installed_kw * 1.5, 2) AS installed_kw_adj,
        CONCAT_WS(' / ', zone, tariff) AS zone_tariff_label
    FROM sites
    WHERE zone = 'Z2'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN installed_kw >= 373.3 THEN 'large' WHEN installed_kw >= 96.9 THEN 'medium' ELSE 'small' END AS installed_kw_tier,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_ym,
        MIN(installed_kw) AS min_installed_kw,
        ROUND(SUM(installed_kw_adj), 2) AS total_installed_kw_adj
    FROM cleaned
    GROUP BY CASE WHEN installed_kw >= 373.3 THEN 'large' WHEN installed_kw >= 96.9 THEN 'medium' ELSE 'small' END, DATE_FORMAT(commissioned_on, 'yyyy-MM')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY total_installed_kw_adj NULLS LAST), 3) AS total_installed_kw_adj_cume_dist,
        DENSE_RANK() OVER (ORDER BY total_installed_kw_adj NULLS LAST) AS total_installed_kw_adj_dense_rank,
        FIRST_VALUE(min_installed_kw) OVER (ORDER BY total_installed_kw_adj NULLS LAST, installed_kw_tier, commissioned_on_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_installed_kw,
        LEAD(min_installed_kw, 3, 0) OVER (ORDER BY total_installed_kw_adj NULLS LAST, installed_kw_tier, commissioned_on_ym) AS next3_min_installed_kw,
        LAG(total_installed_kw_adj) OVER (ORDER BY total_installed_kw_adj NULLS LAST, installed_kw_tier, commissioned_on_ym) AS prev_total_installed_kw_adj,
        ROW_NUMBER() OVER w_all_by_total_installed_kw_ad_3 AS total_installed_kw_adj_seq
    FROM totals
    WINDOW w_all_by_total_installed_kw_ad_3 AS (ORDER BY total_installed_kw_adj NULLS LAST, installed_kw_tier, commissioned_on_ym)
),
leaders AS (
    -- First row per group
    SELECT
        installed_kw_tier,
        commissioned_on_ym,
        min_installed_kw,
        total_installed_kw_adj,
        total_installed_kw_adj_cume_dist,
        total_installed_kw_adj_dense_rank,
        first_min_installed_kw,
        next3_min_installed_kw,
        prev_total_installed_kw_adj
    FROM with_windows
    WHERE total_installed_kw_adj_seq = 1
      AND total_installed_kw_adj_cume_dist > 0.1
)
SELECT
    total_installed_kw_adj_dense_rank,
    installed_kw_tier,
    ROUND(SUM(first_min_installed_kw), 2) AS total_first_min_installed_kw,
    ROUND(STDDEV(prev_total_installed_kw_adj), 2) AS stddev_prev_total_installed_kw_adj
FROM leaders
GROUP BY GROUPING SETS ((total_installed_kw_adj_dense_rank, installed_kw_tier), (total_installed_kw_adj_dense_rank), (installed_kw_tier), ())

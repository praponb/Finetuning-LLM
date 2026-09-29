WITH valid_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        INITCAP(TRIM(tariff)) AS tariff_clean
    FROM sites
    WHERE zone <> 'Z4'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        tariff_clean,
        ROUND(AVG(installed_kw) OVER (PARTITION BY YEAR(commissioned_on_month) ORDER BY installed_kw, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS installed_kw_avg_3,
        installed_kw - LAG(installed_kw) OVER (PARTITION BY YEAR(commissioned_on_month) ORDER BY installed_kw, id) AS installed_kw_change,
        LAST_VALUE(commissioned_on) OVER (PARTITION BY YEAR(commissioned_on_month) ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_commissioned_on,
        DENSE_RANK() OVER (PARTITION BY YEAR(commissioned_on_month) ORDER BY installed_kw) AS installed_kw_dense_rank
    FROM valid_sites
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_label,
        FLOOR(installed_kw_change / 250) * 250 AS installed_kw_change_bucket,
        DATE_ADD(commissioned_on, 365) AS commissioned_on_plus_365d
    FROM ranked
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        tariff_clean,
        installed_kw_avg_3,
        installed_kw_change,
        last_commissioned_on,
        commissioned_on_label,
        installed_kw_change_bucket,
        commissioned_on_plus_365d
    FROM scored
    WHERE installed_kw_dense_rank <= 3
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(installed_kw_change_bucket, 2) OVER (PARTITION BY MONTH(commissioned_on_plus_365d) ORDER BY installed_kw, id) AS prev2_installed_kw_change_bucket,
        SUM(installed_kw_change) OVER w_commissioned_on_plus_365d_mo_2 AS commissioned_on_plus_365d_month_sum_installed_kw,
        COUNT(*) OVER (PARTITION BY MONTH(commissioned_on_plus_365d) ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(installed_kw_change) OVER (PARTITION BY MONTH(commissioned_on_plus_365d) ORDER BY installed_kw, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS installed_kw_change_avg_1_prior
    FROM top_per_group
    WINDOW w_commissioned_on_plus_365d_mo_2 AS (PARTITION BY MONTH(commissioned_on_plus_365d))
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        installed_kw,
        installed_kw_change,
        commissioned_on_label,
        prev2_installed_kw_change_bucket,
        commissioned_on_plus_365d_month_sum_installed_kw,
        DATE_FORMAT(last_commissioned_on, 'MMM yyyy') AS last_commissioned_on_label
    FROM ranked_2
)
-- Final projection
SELECT
    id,
    site_name,
    installed_kw,
    installed_kw_change,
    commissioned_on_label,
    commissioned_on_plus_365d_month_sum_installed_kw,
    last_commissioned_on_label,
    LEAST(installed_kw, installed_kw_change) AS least_installed_kw_installed_kw_change
FROM derived
ORDER BY last_commissioned_on_label DESC

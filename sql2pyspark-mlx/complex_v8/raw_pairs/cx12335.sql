WITH cleaned AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM sites
    WHERE zone IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        tariff_or_unknown,
        FIRST_VALUE(installed_kw) IGNORE NULLS OVER (ORDER BY commissioned_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_installed_kw,
        NTILE(5) OVER (ORDER BY commissioned_on NULLS LAST, id) AS commissioned_on_quintile,
        SUM(installed_kw) OVER (PARTITION BY zone) AS zone_sum_installed_kw,
        NTH_VALUE(commissioned_on, 2) OVER (ORDER BY commissioned_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on,
        DENSE_RANK() OVER w_all_by_commissioned_on AS commissioned_on_dense_rank
    FROM cleaned
    WINDOW w_all_by_commissioned_on AS (ORDER BY commissioned_on NULLS LAST, id)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE commissioned_on_dense_rank <= 5
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(installed_kw) OVER w_commissioned_on_quintile, 2) AS commissioned_on_quintile_avg_installed_kw,
        LEAD(commissioned_on_quintile) OVER w_all_by_commissioned_on_quint AS next_commissioned_on_quintile
    FROM leaders
    WINDOW w_commissioned_on_quintile AS (PARTITION BY commissioned_on_quintile), w_all_by_commissioned_on_quint AS (ORDER BY commissioned_on_quintile, id)
)
SELECT
    commissioned_on_dense_rank,
    tariff_or_unknown,
    commissioned_on_quintile,
    GROUPING(commissioned_on_dense_rank) AS is_total_commissioned_on_dense_rank,
    ARRAY_SORT(COLLECT_SET(zone)) AS zone_list
FROM with_windows
GROUP BY GROUPING SETS ((commissioned_on_dense_rank, tariff_or_unknown, commissioned_on_quintile), (commissioned_on_dense_rank), (tariff_or_unknown))
ORDER BY commissioned_on_dense_rank NULLS LAST, tariff_or_unknown NULLS LAST, commissioned_on_quintile NULLS LAST

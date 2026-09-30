WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        MONTH(commissioned_on) AS commissioned_on_month,
        UPPER(TRIM(tariff)) AS tariff_clean,
        SUBSTRING(TRIM(zone), 1, 2) AS zone_prefix
    FROM sites
    WHERE installed_kw >= 141.6
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(commissioned_on_month / NULLIF(SUM(commissioned_on_month) OVER (PARTITION BY zone, commissioned_on_month), 0), 4) AS commissioned_on_month_share_of_zone_commissioned,
        commissioned_on_month - LAG(commissioned_on_month) OVER (PARTITION BY zone ORDER BY commissioned_on, id) AS commissioned_on_month_change,
        ROW_NUMBER() OVER (PARTITION BY zone ORDER BY commissioned_on, id) AS commissioned_on_seq,
        COUNT(*) OVER w_zone_commissioned_on_month AS rows_in_zone_commissioned_on_month,
        LAST_VALUE(commissioned_on_month) IGNORE NULLS OVER (PARTITION BY zone ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_commissioned_on_month,
        LEAD(commissioned_on_month, 2) OVER w_zone_by_commissioned_on AS next2_commissioned_on_month
    FROM filtered
    WINDOW w_zone_commissioned_on_month AS (PARTITION BY zone, commissioned_on_month), w_zone_by_commissioned_on AS (PARTITION BY zone ORDER BY commissioned_on, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(commissioned_on_seq) OVER (PARTITION BY last_known_commissioned_on_month ORDER BY commissioned_on_month, id) AS prev_commissioned_on_seq,
        LEAD(commissioned_on_month_change) OVER w_zone_by_rows_in_zone_commiss AS next_commissioned_on_month_change,
        RANK() OVER (ORDER BY commissioned_on DESC, id) AS commissioned_on_rank,
        ROW_NUMBER() OVER w_last_known_commissioned_on_m AS rn,
        RANK() OVER (PARTITION BY zone ORDER BY rows_in_zone_commissioned_on_month, id) AS rows_in_zone_commissioned_on_month_rank,
        ROUND(AVG(commissioned_on_month_change) OVER (ORDER BY commissioned_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS commissioned_on_month_change_avg_1_prior
    FROM windowed
    WINDOW w_zone_by_rows_in_zone_commiss AS (PARTITION BY zone ORDER BY rows_in_zone_commissioned_on_month, id), w_last_known_commissioned_on_m AS (PARTITION BY last_known_commissioned_on_month ORDER BY commissioned_on_month, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        tariff_clean,
        commissioned_on_month_share_of_zone_commissioned,
        commissioned_on_month_change,
        commissioned_on_seq,
        rows_in_zone_commissioned_on_month,
        last_known_commissioned_on_month,
        next2_commissioned_on_month,
        prev_commissioned_on_seq,
        rows_in_zone_commissioned_on_month_rank,
        commissioned_on_month_change_avg_1_prior,
        MAX(commissioned_on_month) OVER (PARTITION BY zone, next2_commissioned_on_month) AS zone_next2_commissioned_on_month_max_commissione,
        ROUND(commissioned_on_month_change / NULLIF(SUM(commissioned_on_month_change) OVER (PARTITION BY zone, next2_commissioned_on_month), 0), 4) AS commissioned_on_month_change_share_of_zone_next2,
        ROUND(commissioned_on_month_change - AVG(commissioned_on_month_change) OVER (PARTITION BY zone, next2_commissioned_on_month), 2) AS commissioned_on_month_change_vs_zone_next2_commi
    FROM sequenced
)
-- Subtotals with GROUPING SETS
SELECT
    commissioned_on_seq,
    rows_in_zone_commissioned_on_month_rank,
    GROUPING(commissioned_on_seq) AS is_total_commissioned_on_seq,
    PERCENTILE_APPROX(prev_commissioned_on_seq, 0.5) AS median_prev_commissioned_on_seq
FROM with_windows
GROUP BY GROUPING SETS ((commissioned_on_seq, rows_in_zone_commissioned_on_month_rank), (commissioned_on_seq), ())
ORDER BY commissioned_on_seq NULLS LAST, rows_in_zone_commissioned_on_month_rank NULLS LAST

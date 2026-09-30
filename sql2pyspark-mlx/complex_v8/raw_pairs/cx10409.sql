WITH recent AS (
    -- Filter sites
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        LENGTH(TRIM(tariff)) AS tariff_len,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
    FROM sites
    WHERE zone = 'Z2'
      AND tariff = 'EV'
),
zone_stats AS (
    -- Avg tariff_len per zone
    SELECT zone, AVG(tariff_len) AS avg_tariff_len
    FROM recent
    GROUP BY zone
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_tariff_len,
        ROUND(s.tariff_len - g.avg_tariff_len, 2) AS tariff_len_minus_avg
    FROM recent s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.tariff_len > g.avg_tariff_len
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', commissioned_on) AS days_since_commissioned_on,
        ABS(tariff_len - 10) AS tariff_len_distance
    FROM above_average
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        tariff_len,
        avg_tariff_len,
        tariff_len_minus_avg,
        tariff_len_distance,
        FIRST_VALUE(installed_kw) OVER (PARTITION BY tariff_len, zone ORDER BY avg_tariff_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY installed_kw DESC) AS installed_kw_dense_rank,
        LAG(installed_kw) OVER (PARTITION BY zone ORDER BY installed_kw DESC, id) AS prev_installed_kw,
        RANK() OVER (PARTITION BY tariff_len, zone ORDER BY avg_tariff_len DESC) AS avg_tariff_len_rank,
        MAX(tariff_len_distance) OVER w_commissioned_on_year_tariff AS commissioned_on_year_tariff_len_max_tariff_len_d,
        RANK() OVER w_zone_by_installed_kw AS installed_kw_rank
    FROM scored
    WINDOW w_commissioned_on_year_tariff AS (PARTITION BY YEAR(commissioned_on), tariff_len), w_zone_by_installed_kw AS (PARTITION BY zone ORDER BY installed_kw DESC, id)
)
SELECT
    id,
    zone,
    installed_kw,
    commissioned_on,
    tariff_len,
    avg_tariff_len,
    tariff_len_minus_avg,
    tariff_len_distance,
    installed_kw_dense_rank,
    prev_installed_kw,
    avg_tariff_len_rank,
    installed_kw_rank,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
FROM sequenced
ORDER BY tariff_len DESC, installed_kw_dense_rank, id
LIMIT 20

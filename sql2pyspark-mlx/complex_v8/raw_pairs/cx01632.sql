WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to
    FROM tariff_windows
    WHERE valid_from >= TIMESTAMP '2025-01-24 14:00:00'
      AND tariff NOT IN ('EV', 'TOU')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(valid_from, 'yyyy-MM') AS valid_from_label,
        DATE_FORMAT(valid_to, 'MMM yyyy') AS valid_to_label
    FROM cleaned
    WHERE tariff <> 'FLAT'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(valid_to) IGNORE NULLS OVER (ORDER BY valid_to, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_to,
        FIRST_VALUE(valid_from) IGNORE NULLS OVER (ORDER BY valid_to, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_from,
        RANK() OVER (ORDER BY valid_to) AS valid_to_rank,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER w_all_by_valid_to)) / 60, 1) AS minutes_since_prev_valid_from
    FROM derived
    WINDOW w_all_by_valid_to_ties AS (ORDER BY valid_to), w_all_by_valid_to AS (ORDER BY valid_to, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tariff,
        valid_from,
        valid_from_label,
        valid_to_label,
        first_known_valid_from,
        minutes_since_prev_valid_from,
        DENSE_RANK() OVER (PARTITION BY MONTH(valid_to), TO_DATE(first_known_valid_from) ORDER BY valid_from ASC, id) AS valid_from_dense_rank,
        LAST_VALUE(valid_to_rank) IGNORE NULLS OVER (PARTITION BY MONTH(valid_to), TO_DATE(first_known_valid_from) ORDER BY valid_from ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_to_rank,
        ROUND(AVG(valid_to_rank) OVER (PARTITION BY MONTH(valid_to), TO_DATE(first_known_valid_from) ORDER BY valid_from ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS valid_to_rank_avg_4,
        DENSE_RANK() OVER (PARTITION BY MONTH(valid_to), TO_DATE(first_known_valid_from) ORDER BY valid_from ASC) AS valid_from_dense_rank_2,
        NTILE(10) OVER (PARTITION BY MONTH(valid_to), TO_DATE(first_known_valid_from) ORDER BY valid_from ASC, id) AS valid_from_decile,
        ROUND(valid_to_rank - AVG(valid_to_rank) OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_to)), 2) AS valid_to_rank_vs_tariff_valid_to_month_avg
    FROM with_windows
)
-- Subtotals with CUBE
SELECT
    COALESCE(tariff, 'ALL') AS tariff,
    valid_from_dense_rank_2,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows
FROM with_trends
GROUP BY CUBE(tariff, valid_from_dense_rank_2)
ORDER BY tariff NULLS LAST, valid_from_dense_rank_2 NULLS LAST

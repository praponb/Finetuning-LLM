WITH valid_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug_2
    FROM tariff_windows
    WHERE tariff IN ('TOU', 'FLAT')
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid_tariffs
    WHERE rate > (SELECT AVG(rate) FROM valid_tariffs)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE rate > (SELECT AVG(rate) FROM screened)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        tariff_slug,
        tariff_ref,
        DENSE_RANK() OVER (PARTITION BY tariff ORDER BY valid_from ASC NULLS LAST) AS valid_from_dense_rank,
        SUM(rate) OVER (PARTITION BY TO_DATE(valid_from), tariff) AS valid_from_to_date_tariff_sum_rate,
        LAST_VALUE(valid_to) IGNORE NULLS OVER (PARTITION BY tariff ORDER BY valid_from ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_to
    FROM without_matches
)
SELECT
    valid_from_dense_rank,
    COALESCE(tariff, 'ALL') AS tariff,
    SUM(valid_from_dense_rank) AS total_valid_from_dense_rank,
    SUM(CASE WHEN tariff NOT IN ('FLAT', 'TOU') THEN valid_from_dense_rank ELSE 0 END) AS valid_from_dense_rank_tariff_not_in_flat_tou
FROM windowed
GROUP BY GROUPING SETS ((valid_from_dense_rank, tariff), (valid_from_dense_rank), ())

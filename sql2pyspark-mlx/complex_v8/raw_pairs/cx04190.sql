WITH valid AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        YEAR(valid_from) AS valid_from_year
    FROM tariff_windows
    WHERE tariff = 'EV'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_valid_from_year_ties, 3) AS valid_from_year_percent_rank,
        MAX(valid_from_year) OVER (PARTITION BY valid_from_year) AS valid_from_year_max_valid_from_year
    FROM valid
    WINDOW w_all_by_valid_from_year_ties AS (ORDER BY valid_from_year)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE valid_from_year > (SELECT AVG(valid_from_year) FROM ranked)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        tariff_ref,
        valid_from_year_percent_rank,
        valid_from_year_max_valid_from_year,
        ABS(rate - 5.2) AS rate_distance
    FROM screened
)
-- Final projection
SELECT
    id,
    tariff,
    valid_to,
    tariff_ref,
    valid_from_year_percent_rank
FROM enriched

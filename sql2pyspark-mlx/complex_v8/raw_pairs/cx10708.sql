WITH valid_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        CASE WHEN rate >= 7.6 THEN 'HIGH' WHEN rate >= 5.2 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(valid_to)) / 60, 1) AS minutes_valid_to_to_valid_from,
        SUBSTRING(TRIM(tariff), 1, 3) AS tariff_prefix
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-03-09 05:00:00'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        rate_tier,
        minutes_valid_to_to_valid_from,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(LAG(valid_to) OVER (PARTITION BY tariff ORDER BY rate DESC, id))) / 60, 1) AS minutes_since_prev_valid_to,
        ROUND(PERCENT_RANK() OVER w_tariff_by_rate_ties, 3) AS rate_percent_rank,
        NTH_VALUE(valid_to, 2) OVER (PARTITION BY tariff ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to
    FROM valid_tariffs
    WINDOW w_tariff_by_rate_ties AS (PARTITION BY tariff ORDER BY rate DESC)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        rate_tier,
        minutes_since_prev_valid_to,
        rate_percent_rank,
        DATEDIFF(DATE '2025-12-31', second_valid_to) AS days_since_second_valid_to
    FROM sequenced
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE minutes_since_prev_valid_to > (SELECT AVG(minutes_since_prev_valid_to) FROM enriched)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        rate_percent_rank,
        days_since_second_valid_to,
        NTH_VALUE(minutes_since_prev_valid_to, 2) OVER (PARTITION BY rate_tier ORDER BY rate_percent_rank NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_minutes_since_prev_valid_to,
        COUNT(*) OVER (PARTITION BY rate_tier) AS rows_in_rate_tier,
        ROW_NUMBER() OVER (PARTITION BY rate_tier ORDER BY rate_percent_rank NULLS LAST, id) AS seq_no
    FROM screened
)
-- Final projection
SELECT
    id,
    days_since_second_valid_to,
    second_minutes_since_prev_valid_to,
    rows_in_rate_tier,
    seq_no,
    ROUND(rate_percent_rank * 0.25, 2) AS rate_percent_rank_adj,
    GREATEST(rows_in_rate_tier, seq_no) AS greatest_rows_in_rate_tier_seq_no
FROM ranked
WHERE seq_no <= 8

WITH cleaned AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM tariff_windows
    WHERE valid_to < TIMESTAMP '2025-03-12 23:00:00'
      AND valid_from < TIMESTAMP '2025-02-16 11:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY valid_to DESC, id), 3) AS valid_to_percent_rank,
        ROW_NUMBER() OVER w_tariff_by_valid_to AS seq_no
    FROM cleaned
    WINDOW w_tariff_by_valid_to AS (PARTITION BY tariff ORDER BY valid_to DESC, id)
)
SELECT
    tariff_or_unknown,
    tariff,
    seq_no,
    GROUPING_ID() AS gid,
    SUM(seq_no) FILTER (WHERE tariff IN ('FLAT', 'TOU')) AS seq_no_tariff_in_flat_tou,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(valid_to_percent_rank, 0.5) AS median_valid_to_percent_rank
FROM with_trends
GROUP BY GROUPING SETS ((tariff_or_unknown, tariff, seq_no), (tariff_or_unknown), ())
ORDER BY tariff_or_unknown NULLS LAST, tariff NULLS LAST, seq_no NULLS LAST

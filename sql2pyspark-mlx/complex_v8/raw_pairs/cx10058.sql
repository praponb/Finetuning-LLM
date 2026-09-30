WITH prepared AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
    FROM policyholders
    WHERE joined_on < DATE '2025-09-02'
      AND LENGTH(TRIM(holder_name)) >= 5
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(joined_on) OVER (PARTITION BY region, segment_or_unknown ORDER BY joined_on, id) AS prev_joined_on,
        LEAD(joined_on) OVER w_segment_or_unknown_by_joined AS next_joined_on,
        DENSE_RANK() OVER (PARTITION BY segment_or_unknown ORDER BY joined_on ASC NULLS LAST) AS joined_on_dense_rank,
        NTILE(3) OVER (PARTITION BY region, segment_or_unknown ORDER BY joined_on, id) AS joined_on_tercile,
        RANK() OVER (PARTITION BY segment_or_unknown ORDER BY joined_on) AS joined_on_rank,
        ROUND(PERCENT_RANK() OVER w_region_segment_or_unknown_by_2, 3) AS joined_on_percent_rank,
        LEAD(joined_on) OVER (PARTITION BY segment_or_unknown ORDER BY joined_on, id) AS next_joined_on_2,
        ROW_NUMBER() OVER w_segment_or_unknown_by_joined AS joined_on_seq
    FROM prepared
    WINDOW w_segment_or_unknown_by_joined AS (PARTITION BY segment_or_unknown ORDER BY joined_on ASC NULLS LAST, id), w_region_segment_or_unknown_by_2 AS (PARTITION BY region, segment_or_unknown ORDER BY joined_on)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(joined_on_seq - AVG(joined_on_seq) OVER (PARTITION BY joined_on_tercile), 2) AS joined_on_seq_vs_joined_on_tercile_avg,
        SUM(joined_on_tercile) OVER (PARTITION BY MONTH(next_joined_on), YEAR(next_joined_on_2) ORDER BY joined_on_tercile, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS joined_on_tercile_sum_6_prior,
        MAX(joined_on_dense_rank) OVER (PARTITION BY region ORDER BY joined_on_tercile DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS joined_on_dense_rank_max_2
    FROM windowed
)
-- Final projection
SELECT
    id,
    region,
    joined_on,
    segment_or_unknown,
    joined_on_dense_rank,
    joined_on_tercile,
    joined_on_rank,
    next_joined_on_2,
    joined_on_seq_vs_joined_on_tercile_avg,
    joined_on_tercile_sum_6_prior,
    joined_on_dense_rank_max_2,
    DATEDIFF(DATE '2026-01-01', prev_joined_on) AS days_since_prev_joined_on
FROM windowed_2
WHERE joined_on_seq_vs_joined_on_tercile_avg BETWEEN -8.0 AND 9.0
ORDER BY segment_or_unknown NULLS LAST, joined_on_dense_rank DESC NULLS FIRST

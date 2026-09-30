WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(device) device_upper
    FROM sessions
    WHERE country IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY MONTH(started_at), customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country,
        ROW_NUMBER() OVER w_part row_seq,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country_2
    FROM cleaned
    WINDOW w_part AS (PARTITION BY device ORDER BY started_at DESC, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(row_seq) row_seq_whole,
        COALESCE(country, 'N/A') country_filled
    FROM enriched
    WHERE browser IS NOT NULL
      AND started_at > TIMESTAMP '2023-03-06 23:00:00'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY row_seq_whole DESC NULLS LAST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    device,
    browser,
    CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    PERCENTILE_APPROX(row_seq_whole, ARRAY(0.25, 0.5, 0.75)) AS row_seq_whole_quartiles,
    ROUND(AVG(row_seq_whole), 2) AS avg_row_seq_whole,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    ROUND(SUM(row_seq_whole) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS row_seq_whole_per_firefox,
    APPROX_PERCENTILE(row_seq, 0.95) AS row_seq_p95
FROM deduped
GROUP BY device, browser, CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END
HAVING row_seq_whole_per_firefox < 1000000
ORDER BY num_browsers NULLS LAST;

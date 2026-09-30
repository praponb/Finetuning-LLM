WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        DATEDIFF(CURRENT_DATE(), starts_on) / 365.25 AS years_since_starts_on,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on >= DATE '2023-07-29'
      AND discount_pct BETWEEN 0.21 AND 0.29
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_discount_pct,
        ROW_NUMBER() OVER (ORDER BY dedup_rank DESC, id) AS event_seq
    FROM ranked
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code, channel ORDER BY last4_avg_discount_pct NULLS FIRST) AS rank_in_group
        FROM metrics
    ) ranked
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT promo_code
    FROM latest
    EXCEPT DISTINCT
    SELECT promo_code
    FROM promotions
    WHERE promo_code = 'SPRING10'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.promo_code = k.promo_code
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY last4_count_discount_pct DESC) AS last4_count_discount_pct_dense_rank,
        SUM(event_seq) OVER (PARTITION BY promo_code) AS promo_code_sum_event_seq
    FROM verified
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
        DATE_TRUNC('YEAR', ends_on) AS year_start,
        ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS event_seq_per_spring10,
        ROUND(AVG(event_seq), 2) AS avg_event_seq,
        GROUPING_ID() AS grouping_id
    FROM enriched
    GROUP BY GROUPING SETS ((CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', ends_on)), (CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
)
SELECT
    discount_pct_tier,
    year_start,
    avg_event_seq,
    grouping_id
FROM daily
ORDER BY year_start DESC;

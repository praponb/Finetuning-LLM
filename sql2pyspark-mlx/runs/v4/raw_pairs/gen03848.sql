WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('MONTH', ends_on) AS ends_on_period,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM promotions
    WHERE starts_on >= DATE '2024-03-28'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS seq_no,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY CAST(ends_on_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS discount_pct_avg_7d
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER (ORDER BY discount_pct_avg_7d DESC, id) AS discount_pct_avg_7d_bucket,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on DESC, id) AS row_seq
    FROM scored
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct,
    ends_on_period,
    resolved_channel,
    seq_no,
    discount_pct_avg_7d,
    CASE WHEN promo_code RLIKE '^(SPRING10|WELCOME5)$' THEN 'GROUP_A' WHEN promo_code RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS promo_code_zone
FROM enriched
ORDER BY id

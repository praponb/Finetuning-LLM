WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel,
        SUM(discount_pct) OVER (PARTITION BY MONTH(starts_on) ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_pct,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) discount_pct_share,
        LAST_VALUE(channel) IGNORE NULLS OVER w_hist last_known_channel
    FROM promotions
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('month', ends_on), channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        first_known_channel,
        running_discount_pct,
        last_known_channel,
        dedup_rank,
        SUBSTRING(channel, 1, 3) channel_prefix
    FROM latest
    WHERE starts_on >= TO_DATE('2023-04-15')
),
metrics_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(running_discount_pct) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_running_discount_pct,
        AVG(running_discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) running_discount_pct_avg_2_prior,
        RANK() OVER (ORDER BY running_discount_pct NULLS LAST) running_discount_pct_rank
    FROM filtered
    WHERE running_discount_pct >= 1
)
SELECT
    id,
    promo_code,
    ends_on,
    discount_pct,
    first_known_channel,
    running_discount_pct,
    last_known_channel,
    dedup_rank,
    channel_prefix,
    CASE WHEN promo_code RLIKE '^(BUNDLE|SPRING10)$' THEN 'GROUP_B' WHEN promo_code RLIKE '^(VIP|WELCOME5)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS promo_code_zone
FROM metrics_promotions
ORDER BY id DESC, ends_on DESC

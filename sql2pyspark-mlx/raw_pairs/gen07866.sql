WITH scored AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        MIN(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount_pct,
        MAX(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount_pct
    FROM promotions
    WHERE ends_on BETWEEN '2024-04-09' AND '2024-10-01'
),
enriched AS (
    SELECT
        *,
        ROUND(running_count_discount_pct / SUM(running_count_discount_pct) OVER (PARTITION BY channel), 4) AS running_count_discount_pct_share,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_count_discount_pct DESC, id) AS row_seq,
        RANK() OVER (PARTITION BY MONTH(ends_on), channel ORDER BY running_max_discount_pct DESC NULLS FIRST) AS running_max_discount_pct_rank
    FROM scored
),
filtered AS (
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        running_min_discount_pct,
        running_max_discount_pct,
        running_count_discount_pct_share,
        DATE_TRUNC('MONTH', starts_on) AS starts_on_period,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM enriched
    WHERE ends_on > DATE '2023-10-27'
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 10)
),
flagged AS (
    SELECT *, CASE WHEN starts_on_period - LAG(starts_on_period) OVER (PARTITION BY promo_code ORDER BY starts_on_period, id, promo_id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM matched
),
sessions_promotions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY promo_code ORDER BY starts_on_period, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    channel,
    starts_on,
    discount_pct,
    running_min_discount_pct,
    channel_filled,
    promo_id,
    promo_code,
    promotion_discount_pct
FROM sessions_promotions

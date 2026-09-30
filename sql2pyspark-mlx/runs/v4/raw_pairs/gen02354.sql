WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        FLOOR(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE starts_on >= DATE '2023-05-14'
),
scored AS (
    SELECT
        *,
        LAG(discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS prev_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS event_seq,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY starts_on, id) AS next_discount_pct,
        AVG(discount_pct_whole) OVER (PARTITION BY promo_code) AS promo_code_avg_discount_pct_whole
    FROM valid
),
deduped AS (
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY promo_code ORDER BY promo_code_avg_discount_pct_whole DESC) = 1
),
scored_promotions AS (
    SELECT
        *,
        LAG(discount_pct, 1) OVER w_running AS prev_discount_pct_2,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY prev_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM deduped
    WINDOW w_running AS (PARTITION BY promo_code, channel ORDER BY discount_pct_whole, id)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_whole,
    prev_discount_pct,
    next_discount_pct
FROM scored_promotions
ORDER BY prev_discount_pct, channel;

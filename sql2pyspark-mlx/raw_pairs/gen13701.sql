WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        UPPER(channel) AS channel_upper
    FROM promotions
    WHERE channel IS DISTINCT FROM 'partner'
      AND channel IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        starts_on,
        channel_upper,
        ROW_NUMBER() OVER (PARTITION BY YEAR(ends_on), promo_code ORDER BY ends_on DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS event_seq
    FROM valid
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM scored e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL '14' DAY
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM matched a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
attributed_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed s
    INNER JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '14' DAY
)
SELECT
    id,
    channel,
    starts_on,
    channel_upper,
    row_seq,
    row_seq_2,
    promo_id,
    promo_code,
    discount_pct
FROM attributed_promotions
ORDER BY id;

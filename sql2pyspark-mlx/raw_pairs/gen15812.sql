WITH deduped AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id) AS row_num
        FROM deduped
    ) t
    WHERE row_num = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on DESC, id) AS dedup_rank
        FROM latest
    ) x
    WHERE dedup_rank = 1
),
metrics AS (
    SELECT
        id,
        channel,
        row_num,
        dedup_rank,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY promo_code ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        FIRST_VALUE(channel, TRUE) OVER w_part AS first_known_channel_2
    FROM ranked
    WINDOW w_part AS (PARTITION BY promo_code, channel ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT *, UPPER(channel) AS channel_upper
    FROM metrics
    WHERE channel IS NOT NULL
      AND row_num = 1
)
SELECT
    id,
    channel,
    row_num,
    first_known_channel
FROM cleaned
ORDER BY row_num DESC;

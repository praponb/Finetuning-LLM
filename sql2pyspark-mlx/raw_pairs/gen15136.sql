WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        SUM(discount_pct) OVER w_ordered cumulative_discount_pct,
        FIRST_VALUE(channel, TRUE) OVER w_recent first_known_channel
    FROM promotions
    WINDOW w_ordered AS (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY cumulative_discount_pct NULLS LAST) cumulative_discount_pct_dense_rank,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY cumulative_discount_pct NULLS FIRST) cumulative_discount_pct_dense_rank_2
    FROM enriched
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS dedup_rank
        FROM enriched_promotions
    ) ranked
    WHERE dedup_rank = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM ranked s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        cumulative_discount_pct,
        first_known_channel,
        cumulative_discount_pct_dense_rank,
        cumulative_discount_pct_dense_rank_2,
        dedup_rank,
        promo_id,
        DATEDIFF(CURRENT_DATE(), ends_on) days_since_ends_on
    FROM matched
    WHERE promo_code IS NOT NULL
      AND discount_pct >= 0.12
)
-- Aggregate with conditional metrics
SELECT
    channel,
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT_IF(channel = 'partner') AS partner_events,
    COUNT(*) AS event_count
FROM valid
GROUP BY channel, DATE_TRUNC('WEEK', starts_on)
HAVING COUNT_IF(channel = 'partner') >= 1
ORDER BY channel NULLS LAST;

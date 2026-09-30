WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        SUBSTRING(store_name, 1, 2) store_name_prefix
    FROM stores
    WHERE opened_date >= DATE '2023-02-27'
      AND store_name LIKE '%Flags%'
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.channel,
        x.order_amount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.channel, c.amount AS order_amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY MONTH(opened_date), region), 4) sqft_share,
        FIRST_VALUE(region, TRUE) OVER (ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_region,
        RANK() OVER w_ordered order_amount_rank
    FROM latest_match
    WINDOW w_ordered AS (PARTITION BY YEAR(opened_date), channel ORDER BY order_amount NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(order_amount_rank) OVER (PARTITION BY channel ORDER BY order_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_order_amount_rank,
        AVG(sqft_share) OVER (PARTITION BY channel ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) sqft_share_avg_5_prior,
        DENSE_RANK() OVER w_grp sqft_share_dense_rank,
        SUM(order_amount_rank) OVER (PARTITION BY channel ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_order_amount_rank_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('YEAR', opened_date), region ORDER BY sqft_share NULLS FIRST)
)
SELECT
    id,
    store_name,
    sqft,
    store_name_prefix,
    order_amount,
    sqft_share,
    order_amount_rank,
    sqft_share_avg_5_prior,
    sqft_share_dense_rank
FROM with_windows
ORDER BY sqft_share_dense_rank DESC, order_amount_rank;

WITH with_windows AS (
    SELECT
        id,
        opened_date,
        sqft,
        LAG(sqft, 1) OVER (ORDER BY opened_date, id) AS prev_sqft,
        SUM(sqft) OVER (PARTITION BY MONTH(opened_date), region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_percent_rank
    FROM stores
    WHERE city IN ('Tokyo', 'Bangkok')
),
metrics AS (
    SELECT
        id,
        opened_date,
        sqft_avg_6,
        sqft_percent_rank,
        AVG(sqft_percent_rank) OVER w_running AS last4_avg_sqft_percent_rank,
        MIN(sqft_percent_rank) OVER w_running AS last4_min_sqft_percent_rank,
        SUM(sqft_percent_rank) OVER w_running AS last4_sum_sqft_percent_rank
    FROM with_windows
    WINDOW w_running AS (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics_stores AS (
    SELECT
        id,
        opened_date,
        sqft_avg_6,
        sqft_percent_rank,
        last4_min_sqft_percent_rank,
        last4_sum_sqft_percent_rank,
        DENSE_RANK() OVER (ORDER BY last4_avg_sqft_percent_rank DESC NULLS LAST) AS last4_avg_sqft_percent_rank_dense_rank,
        LAST_VALUE(sqft_percent_rank) IGNORE NULLS OVER w_grp AS last_known_sqft_percent_rank,
        AVG(sqft_avg_6) OVER (ORDER BY sqft_avg_6, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_avg_2_prior,
        AVG(sqft_percent_rank) OVER (PARTITION BY MONTH(opened_date) ORDER BY last4_sum_sqft_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_percent_rank_avg_2,
        NTILE(10) OVER (ORDER BY last4_avg_sqft_percent_rank, id) AS last4_avg_sqft_percent_rank_bucket
    FROM metrics
    WINDOW w_grp AS (ORDER BY last4_avg_sqft_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    SELECT s.*
    FROM metrics_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date < s.opened_date
        )
),
latest_match AS (
    SELECT
        s.*,
        latest.order_order_date,
        latest.discount
    FROM kept s
    JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
report AS (
    SELECT
        id,
        opened_date,
        sqft_avg_6,
        last4_min_sqft_percent_rank,
        last4_sum_sqft_percent_rank,
        sqft_avg_6_avg_2_prior,
        sqft_percent_rank_avg_2,
        last4_avg_sqft_percent_rank_bucket,
        order_order_date
    FROM latest_match
    ORDER BY last4_avg_sqft_percent_rank_bucket NULLS FIRST, sqft_avg_6 DESC
)
SELECT
    id,
    opened_date,
    last4_min_sqft_percent_rank,
    last4_sum_sqft_percent_rank,
    sqft_avg_6_avg_2_prior,
    last4_avg_sqft_percent_rank_bucket,
    order_order_date
FROM report
ORDER BY sqft_avg_6_avg_2_prior;

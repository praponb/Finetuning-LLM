WITH joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.quantity,
        p.launch_date,
        p.category,
        p.price
    FROM inventory src
    LEFT JOIN products p
        ON src.product_id = p.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.quantity,
        a.launch_date,
        a.category,
        b.sqft
    FROM joined a
    LEFT JOIN stores b
        ON a.store_id = b.id
    WHERE b.opened_date <= '2023-08-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity,
        launch_date,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY sqft DESC NULLS FIRST) AS sqft_percent_rank,
        SUM(quantity) OVER (PARTITION BY YEAR(launch_date), category ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('week', launch_date), category ORDER BY quantity ASC NULLS FIRST) AS quantity_percent_rank,
        SUM(quantity) OVER (PARTITION BY category, store_id) AS category_sum_quantity
    FROM enriched
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        quantity,
        sqft_percent_rank,
        quantity_percent_rank,
        category_sum_quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id ASC) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN sqft_percent_rank >= 0.76 THEN 'High' WHEN sqft_percent_rank >= 0.19 THEN 'Medium' ELSE 'Low' END AS sqft_percent_rank_tier,
        CASE WHEN category_sum_quantity >= 1114 THEN 'High' WHEN category_sum_quantity >= 249 THEN 'Medium' ELSE 'Low' END AS category_sum_quantity_band
    FROM latest
    WHERE sqft_percent_rank >= 0.73
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        quantity_percent_rank,
        sqft_percent_rank_tier,
        category_sum_quantity_band,
        AVG(sqft_percent_rank) OVER (ORDER BY category_sum_quantity ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sqft_percent_rank_avg_3_prior,
        NTILE(5) OVER w_part AS sqft_percent_rank_bucket
    FROM valid
    WINDOW w_part AS (ORDER BY sqft_percent_rank DESC, id ASC)
)
-- Grouped metrics
SELECT
    CASE WHEN quantity >= 218 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    COUNT(*) AS event_count,
    BIT_OR(quantity) AS quantity_bit_or,
    COUNT(*) AS num_rows
FROM with_windows
GROUP BY 1
ORDER BY quantity_tier ASC;

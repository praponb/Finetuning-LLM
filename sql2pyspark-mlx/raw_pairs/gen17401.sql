WITH enriched AS (
    SELECT
        id,
        store_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS event_seq,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM inventory
    WHERE quantity BETWEEN 73 AND 158
),
with_dims AS (
    SELECT e.*, s.city
    FROM enriched e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
scored AS (
    SELECT
        id,
        store_id,
        event_seq,
        cumulative_quantity,
        city,
        COUNT(cumulative_quantity) OVER w_recent AS last_restock_period_count_cumulative_quantity,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY cumulative_quantity, id) AS seq_no
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY DATE_TRUNC('MONTH', last_restock), store_id)
),
joined AS (
    SELECT
        a.id,
        a.store_id,
        a.seq_no,
        p.sqft,
        p.store_name,
        p.region
    FROM scored a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
enriched_inventory AS (
    SELECT
        s.id,
        s.store_id,
        s.store_name,
        p.region AS store_region,
        p.sqft AS store_sqft
    FROM joined s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
metrics AS (
    SELECT
        *,
        AVG(store_sqft) OVER (PARTITION BY store_region ORDER BY store_sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS store_sqft_avg_6,
        ROUND(store_sqft / SUM(store_sqft) OVER (PARTITION BY store_id), 4) AS store_sqft_share,
        ROUND(store_sqft / SUM(store_sqft) OVER (PARTITION BY store_id), 4) AS store_sqft_share_2,
        NTILE(5) OVER (PARTITION BY store_region, store_id ORDER BY store_sqft, id) AS store_sqft_bucket
    FROM enriched_inventory
),
latest AS (
    SELECT
        id,
        store_id,
        store_region,
        store_sqft,
        store_sqft_avg_6,
        store_sqft_share,
        store_sqft_share_2
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY store_sqft DESC) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    s.id,
    s.store_id,
    s.store_sqft,
    s.store_sqft_avg_6,
    s.store_sqft_share,
    s.store_sqft_share_2,
    (SELECT MIN(x.units) FROM store_sales x WHERE x.store_id = s.store_id) AS min_store_sale_units
FROM latest s
ORDER BY store_id;

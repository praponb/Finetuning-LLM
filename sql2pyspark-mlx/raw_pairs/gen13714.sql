WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        CEIL(quantity) quantity_whole
    FROM inventory
    WHERE store_id IN (1, 2, 4, 5)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_id,
        product_id,
        last_restock
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rank_in_group
        FROM base
    ) x
    WHERE rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_grp row_seq,
        ROW_NUMBER() OVER w_grp event_seq,
        ROW_NUMBER() OVER w_grp row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY MONTH(last_restock) ORDER BY last_restock, id) event_seq_2
    FROM deduped
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY last_restock DESC, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY row_seq DESC NULLS LAST, id) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        last_restock,
        row_seq,
        event_seq,
        row_seq_2,
        event_seq_2,
        rank_in_group,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY event_seq DESC, id) seq_no_2,
        FIRST_VALUE(seq_no, TRUE) OVER (ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_seq_no
    FROM ranked
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.event_seq,
        src.event_seq_2,
        src.rank_in_group,
        src.seq_no_2,
        src.first_known_seq_no,
        b.brand
    FROM with_windows src
    LEFT JOIN products b
        ON src.product_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_seq_no >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_seq_no_tier,
    CASE WHEN seq_no_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_2_tier,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    PERCENTILE_APPROX(first_known_seq_no, ARRAY(0.25, 0.5, 0.75)) AS first_known_seq_no_quartiles,
    PERCENTILE_APPROX(event_seq_2, ARRAY(0.25, 0.5, 0.75)) AS event_seq_2_quartiles
FROM with_dims
GROUP BY 1, 2
ORDER BY seq_no_2_tier;

WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('month', last_restock) AS last_restock_period,
        DATE_TRUNC('year', last_restock) AS last_restock_period_2
    FROM inventory
    WHERE last_restock > TO_DATE('2023-09-02')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(last_restock_period) OVER (PARTITION BY store_id ORDER BY last_restock_period, id) AS prev_last_restock_period
    FROM base
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_last_restock_period IS NULL OR last_restock_period > prev_last_restock_period + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        last_restock_period,
        last_restock_period_2,
        SUM(session_start_flag) OVER (PARTITION BY store_id ORDER BY last_restock_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock_period NULLS LAST, id) AS rank_in_group
    FROM sessionized
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        last_restock_period_2,
        session_id,
        rank_in_group,
        LEAD(session_id) OVER w_part AS next_session_id,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM deduped
    WHERE rank_in_group = 1
    WINDOW w_part AS (PARTITION BY store_id, product_id ORDER BY last_restock_period_2, id)
),
deduped_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY session_id DESC, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        session_id,
        rank_in_group,
        next_session_id,
        seq_no,
        MIN(session_id) OVER (PARTITION BY store_id, product_id ORDER BY last_restock_period_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_session_id,
        AVG(session_id) OVER (PARTITION BY store_id, product_id ORDER BY last_restock_period_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_session_id,
        COUNT(session_id) OVER (PARTITION BY store_id, product_id ORDER BY last_restock_period_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_session_id,
        MAX(session_id) OVER (PARTITION BY store_id, product_id ORDER BY last_restock_period_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_session_id
    FROM deduped_inventory
)
SELECT
    id,
    session_id,
    rank_in_group,
    seq_no,
    running_min_session_id,
    running_max_session_id
FROM enriched_inventory;

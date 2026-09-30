WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        RANK() OVER w_part units_rank,
        SUM(units) OVER w_part_2 store_id_sum_units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) event_seq
    FROM store_sales
    WINDOW w_part AS (ORDER BY units DESC NULLS LAST), w_part_2 AS (PARTITION BY store_id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) AS rank_in_group
        FROM scored
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(units, 2) OVER (PARTITION BY store_id ORDER BY units_rank, id) prev_units,
        NTILE(4) OVER w_grp event_seq_bucket,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) seq_no
    FROM deduped
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY event_seq DESC, id)
)
SELECT
    id,
    store_id,
    sale_date,
    units,
    revenue,
    units_rank,
    store_id_sum_units,
    rank_in_group,
    prev_units
FROM enriched

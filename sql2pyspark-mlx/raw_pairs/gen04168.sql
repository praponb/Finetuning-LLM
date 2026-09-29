WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        elem.product_id,
        elem.qty
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS elem
),
metrics AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY product_id DESC) AS product_id_dense_rank
    FROM lines
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        qty,
        DENSE_RANK() OVER (ORDER BY product_id) AS product_id_dense_rank_2,
        RANK() OVER (ORDER BY product_id DESC) AS product_id_rank,
        CUME_DIST() OVER (ORDER BY product_id_dense_rank DESC NULLS FIRST) AS product_id_dense_rank_cume_dist,
        RANK() OVER (ORDER BY product_id DESC NULLS FIRST) AS product_id_rank_2,
        DENSE_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank_3
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY product_id_dense_rank_3 DESC NULLS LAST) AS product_id_dense_rank_3_rank,
        CUME_DIST() OVER (ORDER BY product_id_rank DESC) AS product_id_rank_cume_dist
    FROM enriched
    WHERE qty >= 2
)
SELECT
    product_id,
    qty,
    product_id_rank,
    product_id_rank_2,
    product_id_dense_rank_3,
    product_id_dense_rank_3_rank,
    product_id_rank_cume_dist
FROM scored

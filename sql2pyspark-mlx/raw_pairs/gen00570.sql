WITH joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.price,
        p.stock_qty
    FROM store_sales s
    INNER JOIN products p
        ON s.product_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        LEAD(stock_qty, 1) OVER (ORDER BY price ASC, id ASC) next_stock_qty,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) units_share,
        SUM(revenue) OVER (ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue,
        ROW_NUMBER() OVER (ORDER BY units ASC, id ASC) row_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(sale_date), store_id ORDER BY price DESC, id ASC) event_seq
    FROM joined
    WHERE store_id = 6
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY units_share DESC, id ASC) seq_no,
        STDDEV_SAMP(event_seq) OVER (PARTITION BY YEAR(sale_date), store_id) sale_date_year_stddev_samp_event_seq,
        AVG(event_seq) OVER (PARTITION BY MONTH(sale_date), store_id ORDER BY units_share ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) event_seq_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) revenue_dense_rank
    FROM enriched
),
joined_store_sales AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.sale_date,
        a.revenue,
        a.next_stock_qty,
        a.units_share,
        a.row_seq,
        a.event_seq,
        a.revenue_dense_rank,
        p.store_name
    FROM scored a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT a.*, p.opened_date
    FROM joined_store_sales a
    INNER JOIN stores p
        ON a.store_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_store_sales
    QUALIFY RANK() OVER (PARTITION BY store_id ORDER BY next_stock_qty ASC) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    store_id,
    measure,
    value
FROM (
    SELECT
        id,
        store_id,
        revenue,
        units_share
    FROM deduped
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (revenue, units_share)
);

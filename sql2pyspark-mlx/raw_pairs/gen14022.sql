WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365 AS years_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-02-14')
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.years_since_last_restock,
        d.region,
        d.opened_date
    FROM valid src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY opened_date NULLS FIRST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS row_seq,
        CUME_DIST() OVER w_grp AS years_since_last_restock_cume_dist,
        LAG(years_since_last_restock, 1) OVER w_running AS prev_years_since_last_restock,
        DENSE_RANK() OVER w_recent_2 AS years_since_last_restock_dense_rank,
        COUNT(years_since_last_restock) OVER (PARTITION BY MONTH(opened_date), store_id) AS opened_date_month_count_years_since_last_restock
    FROM deduped
    WHERE store_id = 2
    WINDOW w_recent AS (PARTITION BY region ORDER BY opened_date DESC, id), w_grp AS (PARTITION BY region, store_id ORDER BY years_since_last_restock NULLS LAST), w_running AS (PARTITION BY store_id, region ORDER BY opened_date, id), w_recent_2 AS (PARTITION BY YEAR(opened_date), region ORDER BY years_since_last_restock DESC NULLS FIRST)
),
deduped_inventory AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        years_since_last_restock,
        opened_date,
        prev_years_since_last_restock,
        years_since_last_restock_dense_rank,
        opened_date_month_count_years_since_last_restock
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, store_id ORDER BY prev_years_since_last_restock NULLS LAST, id) = 1
),
scored_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(prev_years_since_last_restock) OVER (PARTITION BY store_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_years_since_last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY years_since_last_restock_dense_rank DESC, id) AS seq_no,
        AVG(years_since_last_restock) OVER (PARTITION BY store_id ORDER BY years_since_last_restock, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS years_since_last_restock_avg_5
    FROM deduped_inventory
)
SELECT
    id,
    years_since_last_restock,
    opened_date,
    prev_years_since_last_restock,
    years_since_last_restock_dense_rank,
    cumulative_prev_years_since_last_restock
FROM scored_inventory

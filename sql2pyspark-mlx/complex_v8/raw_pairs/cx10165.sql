WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-12-02'
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        p.region AS holder_region
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, product), 2) AS holder_id_product_avg_premium,
        COUNT(*) OVER (PARTITION BY holder_id, product) AS rows_in_holder_id_product,
        MIN(premium) OVER (PARTITION BY holder_id, product) AS holder_id_product_min_premium,
        FIRST_VALUE(premium) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date_ties, 3) AS end_date_percent_rank,
        NTILE(4) OVER (ORDER BY end_date ASC, id) AS end_date_quartile,
        SUM(premium) OVER (PARTITION BY holder_id, product) AS holder_id_product_sum_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id, product), 2) AS premium_vs_holder_id_product_avg,
        DENSE_RANK() OVER w_all_by_end_date_ties AS end_date_dense_rank
    FROM joined_holder
    WINDOW w_all_by_end_date_ties AS (ORDER BY end_date DESC), w_holder_id_product AS (PARTITION BY holder_id, product)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        product,
        premium,
        status,
        holder_id_product_avg_premium,
        rows_in_holder_id_product,
        holder_id_product_min_premium,
        first_premium,
        end_date_percent_rank,
        end_date_quartile,
        holder_id_product_sum_premium,
        premium_vs_holder_id_product_avg,
        end_date_dense_rank,
        ROUND(premium * 0.9, 2) AS premium_adj,
        GREATEST(end_date_dense_rank, end_date_percent_rank) AS greatest_end_date_dense_rank_end_date_percent_ra,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM with_windows
)
SELECT
    id,
    premium,
    status,
    holder_id_product_avg_premium,
    rows_in_holder_id_product,
    first_premium,
    end_date_percent_rank,
    end_date_quartile,
    holder_id_product_sum_premium,
    premium_vs_holder_id_product_avg,
    end_date_dense_rank,
    premium_adj,
    greatest_end_date_dense_rank_end_date_percent_ra,
    product_or_unknown,
    INITCAP(TRIM(product_or_unknown)) AS product_or_unknown_clean,
    ROUND(premium_adj * 1.07, 2) AS premium_adj_adj
FROM labeled

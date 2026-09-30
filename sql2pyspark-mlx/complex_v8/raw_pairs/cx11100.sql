WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY premium, id) AS premium_seq
    FROM recent_policys
    WINDOW w_product_holder_id_by_premium AS (PARTITION BY product, holder_id ORDER BY premium, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE premium_seq <= 2
),
annotated AS (
    -- Add calculated columns
    SELECT *, CONCAT(product, '#', CAST(id AS STRING)) AS product_ref_2
    FROM leaders
)
SELECT
    holder_id,
    premium_seq,
    COALESCE(product, 'ALL') AS product,
    GROUPING_ID() AS gid,
    SUM(rows_in_product) AS total_rows_in_product,
    COUNT(DISTINCT product) AS distinct_products,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids
FROM annotated
GROUP BY GROUPING SETS ((holder_id, premium_seq, product), (holder_id), (premium_seq), ())

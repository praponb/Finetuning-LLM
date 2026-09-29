WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CASE WHEN premium >= 3521.8 THEN 'HIGH' WHEN premium >= 2096.6 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE product IN ('HEALTH', 'AUTO', 'LIFE')
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.premium_tier,
        p.region AS holder_region
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
unique_rows AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        premium_tier,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM combined_details_holder
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Add calculated columns
    SELECT *, MONTH(start_date) AS start_date_month
    FROM unique_rows
    WHERE holder_region = 'South'
),
latest_per_key AS (
    -- Keep the earliest row per holder_region, holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        premium_tier,
        holder_region,
        start_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_region, holder_id ORDER BY start_date, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(start_date_month) OVER (PARTITION BY holder_region), 2) AS holder_region_avg_start_date_month,
        COUNT(*) OVER (PARTITION BY holder_region) AS rows_in_holder_region,
        RANK() OVER w_all_by_start_date_ties AS start_date_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_start_date, 3) AS start_date_percent_rank
    FROM latest_per_key
    WINDOW w_all_by_start_date_ties AS (ORDER BY start_date DESC), w_all_by_start_date AS (ORDER BY start_date DESC, id)
)
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    holder_region,
    start_date_month,
    rows_in_holder_region,
    start_date_rank,
    start_date_percent_rank
FROM metrics
ORDER BY id NULLS LAST, holder_id NULLS LAST

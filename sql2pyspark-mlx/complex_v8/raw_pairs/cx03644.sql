WITH scoped AS (
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
    WHERE product IN ('LIFE', 'TRAVEL', 'AUTO')
      AND start_date < DATE '2025-03-06'
),
annotated AS (
    -- Add calculated columns
    SELECT *, ABS(premium - 2824.5) AS premium_distance
    FROM scoped
    WHERE holder_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        premium_distance - LAG(premium_distance) OVER w_start_date_year_by_start_dat AS premium_distance_change,
        COUNT(*) OVER (PARTITION BY MONTH(start_date), holder_id) AS rows_in_start_date_month_holder_id,
        RANK() OVER (ORDER BY end_date ASC, id) AS end_date_rank,
        NTILE(4) OVER (ORDER BY end_date ASC, id) AS end_date_quartile
    FROM annotated
    WINDOW w_start_date_year_by_start_dat AS (PARTITION BY YEAR(start_date) ORDER BY start_date ASC, id)
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS last_claim_date,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM windowed s
)
SELECT id, metric_name, amount
FROM (
    SELECT id, end_date_quartile, claim_count
    FROM summarized_rows
) src
UNPIVOT (
    amount FOR metric_name IN (end_date_quartile AS m_end_date_quartile, claim_count AS claim_count_v)
) unp
ORDER BY id, metric_name

WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM policies
    WHERE start_date IS NOT NULL
      AND product = 'AUTO'
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.end_date_label,
        p.segment AS holder_segment
    FROM scoped s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment IN ('RETAIL', 'VIP')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        end_date_label,
        RANK() OVER (ORDER BY end_date DESC, id) AS end_date_rank,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_end_date_ties, 3) AS end_date_percent_rank,
        FIRST_VALUE(end_date) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        ROW_NUMBER() OVER (ORDER BY end_date DESC, id) AS seq_no,
        DATEDIFF(end_date, LAG(end_date) OVER w_holder_id_by_end_date) AS days_since_prev_end_date
    FROM combined_details_holder
    WINDOW w_holder_id_by_end_date_ties AS (PARTITION BY holder_id ORDER BY end_date DESC), w_holder_id_by_end_date AS (PARTITION BY holder_id ORDER BY end_date DESC, id)
),
unique_rows AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        end_date,
        end_date_label,
        end_date_rank,
        end_date_percent_rank,
        first_end_date,
        seq_no,
        days_since_prev_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY first_end_date, id) AS recency_rank
        FROM windowed
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN product IN ('TRAVEL', 'AUTO') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        DATEDIFF(DATE '2025-12-31', first_end_date) AS days_since_first_end_date
    FROM unique_rows
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.reported_at < TIMESTAMP '2025-03-02 14:00:00'
              AND c.claim_date >= DATE '2024-04-09'
        )
      AND s.product NOT IN ('LIFE', 'HOME')
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(start_date) AS first_policy_start_date,
        ROUND(AVG(premium), 2) AS avg_policy_premium
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_start_date,
        p.avg_policy_premium
    FROM filtered s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
)
SELECT
    id,
    holder_id,
    end_date,
    end_date_rank,
    seq_no,
    days_since_prev_end_date,
    product_group,
    days_since_end_date,
    days_since_first_end_date,
    avg_policy_premium,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown,
    COALESCE(product_group, 'UNKNOWN') AS product_group_or_unknown
FROM summarized_rows
WHERE days_since_first_end_date < 525
ORDER BY product_group

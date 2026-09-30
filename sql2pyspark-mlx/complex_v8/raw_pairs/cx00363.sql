WITH recent_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status IN ('EXPIRED', 'ACTIVE')
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS policy_status_count,
        COUNT(*) AS policy_count,
        COUNT(*) FILTER (WHERE product = 'TRAVEL') AS policy_product_count
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_status_count, 0) AS policy_status_count,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_product_count, 0) AS policy_product_count
    FROM recent_policys s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
deduped AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        policy_status_count,
        policy_count,
        policy_product_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_policy_count_by_policy_produ, 3) AS policy_product_count_cume_dist,
        SUM(policy_product_count) OVER w_policy_status_count AS policy_status_count_sum_policy_product_count,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY policy_status_count), 0), 4) AS premium_share_of_policy_status_count
    FROM deduped
    WINDOW w_policy_count_by_policy_produ AS (PARTITION BY policy_count ORDER BY policy_product_count, id), w_policy_status_count AS (PARTITION BY policy_status_count)
)
SELECT
    id,
    holder_id,
    product,
    end_date,
    policy_product_count,
    policy_product_count_cume_dist,
    policy_status_count_sum_policy_product_count,
    ROUND(premium * 1.5, 2) AS premium_adj,
    DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
FROM with_trends
ORDER BY holder_id, id
LIMIT 20

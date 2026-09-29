WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM policies
    WHERE status = 'CANCELLED'
      AND product = 'HOME'
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.start_date_label,
        p.joined_on AS holder_joined_on
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.start_date_label,
        s.holder_joined_on,
        p.joined_on AS holder_joined_on_2,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM with_details_holder s
    JOIN policyholders p ON p.id = s.holder_id AND p.risk_score BETWEEN 0.1 AND 0.8
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.start_date_label,
        s.holder_joined_on,
        s.holder_joined_on_2,
        s.holder_region,
        s.holder_risk_score,
        p.holder_name,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on_3
    FROM joined s
    JOIN policyholders p ON p.id = s.holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MAX(end_date) AS last_policy_end_date,
        MAX(end_date) AS last_policy_end_date_2
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.last_policy_end_date,
        p.last_policy_end_date_2
    FROM with_details s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
unique_rows AS (
    -- Keep the latest row per holder_id, holder_region
    SELECT
        id,
        holder_id,
        start_date,
        start_date_label,
        holder_joined_on,
        holder_joined_on_2,
        holder_region,
        holder_risk_score,
        holder_name,
        holder_segment,
        holder_joined_on_3,
        last_policy_end_date,
        last_policy_end_date_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, holder_region ORDER BY holder_joined_on_3 DESC, id DESC) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Label and score rows
    SELECT *, COALESCE(holder_segment, 'UNKNOWN') AS holder_segment_or_unknown
    FROM unique_rows
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    start_date_label,
    holder_joined_on,
    holder_joined_on_2,
    holder_region,
    holder_name,
    holder_segment,
    holder_joined_on_3,
    last_policy_end_date_2,
    holder_segment_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(start_date_label), ' ', '_')) AS start_date_label_slug
FROM scored
WHERE start_date_label IS NOT NULL

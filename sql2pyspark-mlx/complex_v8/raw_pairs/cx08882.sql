WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', product, status) AS product_status_label
    FROM policies
    WHERE status = 'LAPSED'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, MIN(start_date) AS first_policy_start_date
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_start_date
    FROM valid_policys s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        first_policy_start_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        DATEDIFF(first_policy_start_date, end_date) AS days_end_date_to_first_policy_start_date
    FROM with_counts
    WHERE product_status_label IS NOT NULL
)
SELECT
    id,
    holder_id,
    product,
    end_date,
    first_policy_start_date,
    status_ref,
    LOWER(REGEXP_REPLACE(TRIM(status_ref), ' ', '_')) AS status_ref_slug
FROM enriched
ORDER BY product DESC, holder_id NULLS LAST

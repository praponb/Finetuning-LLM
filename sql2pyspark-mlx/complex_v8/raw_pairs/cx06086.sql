WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        UPPER(TRIM(status)) AS status_clean,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM policies
    WHERE product <> 'HOME'
),
latest_per_key AS (
    -- Keep the earliest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        status_clean,
        end_date_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY start_date, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        s.status_clean,
        s.end_date_dayofweek,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM latest_per_key s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY holder_id ORDER BY end_date_dayofweek DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY end_date_dayofweek DESC, id), 3) AS end_date_dayofweek_percent_rank
    FROM joined_holder
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score_2,
        p.joined_on AS holder_joined_on
    FROM with_trends s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(holder_region, 'UNKNOWN') AS holder_region_or_unknown,
        ROUND(holder_risk_score * 0.9, 2) AS holder_risk_score_adj
    FROM with_details_holder
    WHERE end_date >= DATE '2025-12-05'
),
tagged AS (
    -- Label and score rows
    SELECT *, INITCAP(TRIM(status_clean)) AS status_clean_clean
    FROM scored
)
SELECT
    id,
    holder_id,
    end_date,
    status,
    status_clean,
    end_date_dayofweek,
    holder_region,
    end_date_dayofweek_percent_rank,
    holder_segment,
    holder_risk_score_2,
    holder_region_or_unknown,
    holder_risk_score_adj,
    status_clean_clean,
    ABS(running_count - 10) AS running_count_distance,
    SUBSTRING(TRIM(status), 1, 2) AS status_prefix
FROM tagged
ORDER BY status, status_clean_clean

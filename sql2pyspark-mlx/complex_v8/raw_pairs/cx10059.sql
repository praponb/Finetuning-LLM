WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
    WHERE YEAR(end_date) = 2026
      AND product = 'LIFE'
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id IS NOT NULL
              AND c.approved_amount BETWEEN 0.0 AND 15857.9
        )
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.days_since_end_date,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM with_activity s
    JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(days_since_end_date / NULLIF(SUM(days_since_end_date) OVER (PARTITION BY status), 0), 4) AS days_since_end_date_share_of_status,
        NTILE(3) OVER (PARTITION BY status, holder_id ORDER BY holder_risk_score DESC, id) AS holder_risk_score_tercile,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY status), 0), 4) AS holder_risk_score_share_of_status,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status
    FROM enriched_holder
),
with_activity_2 AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM ranked s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(holder_risk_score / NULLIF(days_since_end_date, 0), 3) AS holder_risk_score_per_days_since_end_date,
        CASE WHEN days_since_end_date >= 490 THEN 'large' WHEN days_since_end_date >= 280 THEN 'medium' ELSE 'small' END AS days_since_end_date_tier
    FROM with_activity_2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        status,
        days_since_end_date,
        holder_region,
        days_since_end_date_share_of_status,
        holder_risk_score_share_of_status,
        days_since_end_date_tier,
        RANK() OVER w_holder_id_by_holder_risk_sco AS holder_risk_score_share_of_status_rank,
        MAX(rows_in_status) OVER (PARTITION BY holder_id ORDER BY holder_risk_score_share_of_status, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rows_in_status_max_4,
        MAX(days_since_end_date) OVER (PARTITION BY holder_id, holder_risk_score_tercile) AS holder_id_holder_risk_score_tercile_max_days_sin,
        SIZE(COLLECT_SET(holder_region) OVER (PARTITION BY holder_id, holder_risk_score_tercile)) AS distinct_holder_region_in_holder_id_holder_risk_
    FROM labeled
    WINDOW w_holder_id_by_holder_risk_sco AS (PARTITION BY holder_id ORDER BY holder_risk_score_share_of_status, id)
)
SELECT
    id,
    holder_id,
    days_since_end_date,
    holder_region,
    days_since_end_date_share_of_status,
    holder_risk_score_share_of_status,
    days_since_end_date_tier,
    rows_in_status_max_4,
    holder_id_holder_risk_score_tercile_max_days_sin,
    distinct_holder_region_in_holder_id_holder_risk_,
    CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
    CONCAT(status, '#', CAST(days_since_end_date AS STRING)) AS status_ref
FROM sequenced
WHERE holder_region <> 'East'
ORDER BY distinct_holder_region_in_holder_id_holder_risk_ DESC, holder_region DESC

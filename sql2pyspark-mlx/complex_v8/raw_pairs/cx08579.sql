WITH valid_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        MONTH(joined_on) AS joined_on_month,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        region,
        segment,
        joined_on_month,
        risk_score_filled,
        CONCAT_WS(' / ', holder_name, segment) AS holder_name_segment_label
    FROM valid_holders
    WHERE joined_on_month BETWEEN 2 AND 9
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(joined_on_month) OVER w_segment AS segment_max_joined_on_month,
        ROUND(risk_score_filled / NULLIF(SUM(risk_score_filled) OVER w_segment, 0), 4) AS risk_score_filled_share_of_segment,
        ROUND(joined_on_month / NULLIF(SUM(joined_on_month) OVER w_segment, 0), 4) AS joined_on_month_share_of_segment,
        FIRST_VALUE(joined_on_month) IGNORE NULLS OVER (ORDER BY risk_score_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on_month
    FROM derived
    WINDOW w_segment AS (PARTITION BY segment)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE risk_score_filled_share_of_segment < (SELECT MAX(risk_score_filled_share_of_segment) FROM ranked)
),
unique_rows AS (
    -- Keep the latest row per region
    SELECT
        id,
        region,
        segment,
        joined_on_month,
        risk_score_filled,
        holder_name_segment_label,
        segment_max_joined_on_month,
        risk_score_filled_share_of_segment,
        joined_on_month_share_of_segment,
        first_known_joined_on_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY segment_max_joined_on_month DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per region
    SELECT
        id,
        region,
        segment,
        joined_on_month,
        risk_score_filled,
        holder_name_segment_label,
        segment_max_joined_on_month,
        risk_score_filled_share_of_segment,
        joined_on_month_share_of_segment,
        first_known_joined_on_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY joined_on_month_share_of_segment DESC, id DESC) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    region,
    segment,
    joined_on_month,
    risk_score_filled,
    holder_name_segment_label,
    segment_max_joined_on_month,
    risk_score_filled_share_of_segment,
    joined_on_month_share_of_segment,
    first_known_joined_on_month,
    COALESCE(region, 'UNKNOWN') AS region_or_unknown,
    LENGTH(TRIM(segment)) AS segment_len
FROM unique_rows_2
ORDER BY region_or_unknown, joined_on_month_share_of_segment DESC

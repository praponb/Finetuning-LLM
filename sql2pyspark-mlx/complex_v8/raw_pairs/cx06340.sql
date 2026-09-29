WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        QUARTER(closed_at) AS closed_at_quarter
    FROM tickets
    WHERE (opened_at < TIMESTAMP '2025-03-02 10:00:00' OR channel = 'web')
),
annotated AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM valid
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label,
        CASE WHEN closed_at_quarter >= 3 THEN 'large' WHEN closed_at_quarter >= 2 THEN 'medium' ELSE 'small' END AS closed_at_quarter_tier
    FROM annotated
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        opened_at,
        closed_at,
        channel_prefix,
        minutes_opened_at_to_closed_at,
        closed_at_quarter_tier,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at,
        SUBSTRING(TRIM(channel_prefix), 1, 3) AS channel_prefix_prefix
    FROM labeled
    WHERE opened_at < TIMESTAMP '2025-03-02 10:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(hours_opened_at_to_closed_at) OVER (PARTITION BY closed_at_quarter_tier, DATE_TRUNC('MONTH', opened_at)) AS closed_at_quarter_tier_opened_at_month_sum_hours,
        ROW_NUMBER() OVER (ORDER BY hours_opened_at_to_closed_at ASC, id) AS hours_opened_at_to_closed_at_seq,
        ROUND(PERCENT_RANK() OVER w_all_by_minutes_opened_at_to, 3) AS minutes_opened_at_to_closed_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_at), closed_at_quarter_tier ORDER BY minutes_opened_at_to_closed_at NULLS LAST, id) AS rn
    FROM enriched
    WINDOW w_all_by_minutes_opened_at_to AS (ORDER BY minutes_opened_at_to_closed_at ASC), w_all_by_hours_opened_at_to_cl AS (ORDER BY hours_opened_at_to_closed_at ASC, id)
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at,
    channel_prefix,
    closed_at_quarter_tier,
    hours_opened_at_to_closed_at,
    closed_at_quarter_tier_opened_at_month_sum_hours,
    hours_opened_at_to_closed_at_seq,
    minutes_opened_at_to_closed_at_percent_rank,
    rn
FROM windowed
WHERE minutes_opened_at_to_closed_at_percent_rank BETWEEN 0.1 AND 0.9
ORDER BY hours_opened_at_to_closed_at_seq DESC NULLS FIRST

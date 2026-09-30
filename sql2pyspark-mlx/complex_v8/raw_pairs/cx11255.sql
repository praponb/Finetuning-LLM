SELECT source, device, 'play' AS src
FROM plays
UNION ALL
SELECT device AS source, source AS device, 'play_2' AS src
FROM plays
ORDER BY device DESC

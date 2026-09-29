SELECT device, played_at
FROM plays
WHERE device <> 'phone'
UNION
SELECT device, played_at
FROM plays
WHERE is_skipped = TRUE
ORDER BY played_at DESC, device

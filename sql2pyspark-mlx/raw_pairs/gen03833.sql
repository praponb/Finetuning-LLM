SELECT *
FROM sessions
WHERE browser IN ('Edge', 'Safari') OR device = 'mobile'
ORDER BY id DESC;

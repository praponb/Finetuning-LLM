SELECT customer_id, device, UPPER(browser) AS browser_upper FROM sessions WHERE browser IN ('Chrome', 'Edge', 'Firefox') AND browser <> 'Edge' ORDER BY customer_id

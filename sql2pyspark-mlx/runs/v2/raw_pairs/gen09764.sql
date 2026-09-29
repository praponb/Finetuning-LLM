SELECT country FROM customers WHERE is_active = FALSE EXCEPT SELECT country FROM sessions WHERE device IN ('mobile', 'tablet', 'desktop')

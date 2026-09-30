SELECT shipped_date, carrier, order_id, LAST_DAY(delivered_date) AS delivered_date_month_end, ADD_MONTHS(shipped_date, 3) AS shipped_date_plus_months FROM shipments WHERE cost > 65;

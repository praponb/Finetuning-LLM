select shipped_date from shipments except select sale_date from store_sales where units > 12 order by shipped_date;

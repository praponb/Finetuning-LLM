select status, shipped_date from shipments where year(delivered_date) >= 2025 order by shipped_date;

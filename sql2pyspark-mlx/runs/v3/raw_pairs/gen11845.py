from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Riv%") & F.col("opened_date").between("2024-01-01", "2024-06-01"))
    .select("city")
)

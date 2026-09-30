from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 172)
    .select("last_restock")
    .intersect(
        df_stores
        .filter(F.col("store_name").like("Riv%"))
        .select("opened_date")
    )
)

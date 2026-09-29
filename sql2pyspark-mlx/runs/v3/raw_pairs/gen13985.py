from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("store_id", "status")
    .intersect(df_orders.filter(F.col("amount") < 590).select("store_id", "status"))
)

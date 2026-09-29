from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.filter(~F.col("channel").isin(["partner", "store", "mobile"])).alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_semi")
    .select(F.col("stores.id"), F.col("stores.city"))
    .orderBy("city")
)

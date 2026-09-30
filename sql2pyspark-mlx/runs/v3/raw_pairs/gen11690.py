from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.filter(~F.col("status").isin(["COMPLETED", "SHIPPED", "PENDING"])).alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_semi")
    .select(F.col("stores.id"), F.col("stores.store_name"), F.col("stores.opened_date"))
    .orderBy("store_name", "id")
)

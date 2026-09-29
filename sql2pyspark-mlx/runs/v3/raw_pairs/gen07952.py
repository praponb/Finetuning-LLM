from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(~F.col("stores.city").isin(["Singapore", "New York", "Tokyo"]) & (F.col("orders.channel") != "store"))
    .select(
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.col("stores.region"),
        F.col("orders.amount"),
    )
    .orderBy("opened_date")
)

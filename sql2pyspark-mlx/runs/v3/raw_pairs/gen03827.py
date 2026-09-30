from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .filter(F.col("stores.sqft") >= 10899)
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .filter(F.col("cnt") >= 2)
    .orderBy("cnt", "opened_month")
    .limit(5)
)

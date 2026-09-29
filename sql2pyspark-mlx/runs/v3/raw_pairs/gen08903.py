from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 6)
    .select("store_id", "last_restock_month", "cnt")
    .orderBy(F.col("store_id").desc(), "cnt")
)

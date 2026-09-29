from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & (F.col("shipped_date") > "2025-09-29"))
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.count("cost").alias("cost_count"),
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
    )
    .orderBy("cost_count")
)

from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy(F.col("cost_range").desc())
)

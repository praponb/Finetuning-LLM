from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy(F.col("flash_count").desc(), "status_group")
    .limit(3)
)

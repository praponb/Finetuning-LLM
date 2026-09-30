from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("kerry_count") >= 6)
    .select("carrier", "kerry_count", "cost_range")
    .orderBy("cost_range")
)

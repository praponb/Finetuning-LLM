from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2024-10-30") & (F.col("carrier") == "DHL") & (F.col("delivered_date") <= "2025-10-11"))
    .groupBy("carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
    )
    .orderBy(F.col("kerry_count").desc(), F.col("avg_cost").desc())
)

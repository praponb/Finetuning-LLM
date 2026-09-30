from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .orderBy("avg_cost")
)

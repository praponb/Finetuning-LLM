from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 86) & F.col("carrier").isNotNull())
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") < 78)
    .orderBy("avg_cost")
)

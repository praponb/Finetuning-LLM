from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([2, 11, 16, 23]))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("avg_cost") < 48)
    .select("shipped_quarter", "avg_cost", "max_cost")
    .orderBy(F.col("max_cost").desc(), "shipped_quarter")
    .limit(10)
)

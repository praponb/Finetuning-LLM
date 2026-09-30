from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-11-01")
    .groupBy("carrier", "status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") < 90)
    .select("carrier", "status", "total_cost", "min_cost")
    .orderBy(F.col("min_cost").desc())
)

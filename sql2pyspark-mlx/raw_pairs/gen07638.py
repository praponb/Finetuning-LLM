from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED", "LOST"]))
    .groupBy("status")
    .agg(
        F.count("*").alias("total_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_count") > 1)
    .orderBy(F.col("total_count").desc())
)

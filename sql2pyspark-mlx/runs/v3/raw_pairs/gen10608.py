from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("total_cost") > 160)
    .orderBy(F.col("total_cost").desc(), "status")
    .limit(3)
)

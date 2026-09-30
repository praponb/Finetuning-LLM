from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") > 29)
    .select("status", "unique_orders", "min_cost")
)

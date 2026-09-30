from pyspark.sql import functions as F

result = (
    df_orders
    .cube("status", "store_id")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.countDistinct("discount").alias("unique_statuss"),
    )
    .orderBy(F.col("store_id").desc())
)

from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"),
        F.when(F.col("status").isin(["SHIPPED", "COMPLETED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)

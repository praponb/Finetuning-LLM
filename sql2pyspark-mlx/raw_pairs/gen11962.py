from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["SHIPPED", "PENDING"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
    .filter(F.col("avg_discount") < 64)
    .orderBy("status_group")
)

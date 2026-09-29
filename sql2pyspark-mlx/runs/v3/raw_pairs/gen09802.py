from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["RETURNED", "PENDING"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("status_group", "total_amount")
)

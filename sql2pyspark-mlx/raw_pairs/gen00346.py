from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["COMPLETED", "PENDING"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .filter(F.col("total_shipping_fee") >= 90)
)

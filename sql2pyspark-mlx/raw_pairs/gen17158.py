from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")

ranked = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("discount") >= 0.39, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("shipping_fee_per_store"),
        F.max(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee"))).alias("completed_max_shipping_fee"),
    )
    .orderBy(F.col("discount_tier").desc())
)

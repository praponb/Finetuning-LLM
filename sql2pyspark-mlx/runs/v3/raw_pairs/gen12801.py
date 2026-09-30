from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.38) & ((F.col("shipping_fee") > 37) | F.col("store_id").isNull()))
    .agg(
        F.countDistinct("discount").alias("unique_stores"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("shipping_fee")).otherwise(0)).alias("cancelled_shipping_fee"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc())
)

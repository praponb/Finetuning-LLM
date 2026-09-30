from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("amount")).otherwise(0)).alias("partner_amount"),
    )
    .orderBy(F.col("partner_amount").desc(), F.col("store_id").desc())
)

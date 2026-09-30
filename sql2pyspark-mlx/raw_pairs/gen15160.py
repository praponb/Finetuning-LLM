from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([11, 18])), "left_semi")
)

result = (
    eligible
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("returned_amount").desc())
)

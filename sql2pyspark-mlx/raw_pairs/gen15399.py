from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.sum("amount").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_id",
        F.col("SUCCESS_total").alias("success_total"),
        F.col("SUCCESS_cnt").alias("success_cnt"),
        F.col("REFUNDED_total").alias("refunded_total"),
        F.col("REFUNDED_cnt").alias("refunded_cnt"),
    )
)

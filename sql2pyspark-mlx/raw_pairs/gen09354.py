from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-08-07"))) & (F.col("order_id") == 31))
    .withColumn("method_status", F.concat("method", F.lit(" - "), "status"))
)

result = (
    cleaned
    .select("id", "order_id")
    .subtract(
        df_payments
        .filter(F.col("amount") <= 880)
        .select("id", "order_id")
    )
)

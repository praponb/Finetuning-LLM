from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-09-03"))) & F.col("order_id").isNotNull())
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM-dd"))
    .withColumn("clean_method", F.trim("method"))
)

result = (
    valid
    .select("method", "status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([17, 29, 39]))
        .select("method", "status")
    )
)

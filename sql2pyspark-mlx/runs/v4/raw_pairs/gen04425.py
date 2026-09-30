from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "order_id").orderBy("amount", "id")

metrics = (
    df_payments
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w))
    .withColumn("amount_bucket", F.ntile(5).over(w2))
)

base = (
    metrics
    .filter(F.col("paid_at") > F.lit("2024-03-19").cast("date"))
    .select(
        "id",
        "method",
        "amount",
        "last_known_amount",
        "amount_bucket",
        F.ceil("last_known_amount").alias("last_known_amount_whole"),
    )
)

base_payments = (
    base
    .filter(~F.col("method").isin(["promptpay", "bank_transfer"]))
    .select(
        "id",
        "method",
        "amount",
        "last_known_amount",
        "last_known_amount_whole",
        (F.col("last_known_amount_whole") - F.col("amount")).alias("last_known_amount_whole_minus_amount"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

result = (
    base_payments
    .select(
        "id",
        "amount",
        "last_known_amount_whole",
        "last_known_amount_whole_minus_amount",
        "resolved_method",
    )
    .orderBy(F.col("id").desc(), "last_known_amount_whole")
)

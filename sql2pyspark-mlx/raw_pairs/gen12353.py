from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

enriched = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.row_number().over(w).alias("seq_no"),
        F.max("amount").over(w2).alias("amount_max_7d"),
    )
)

kept_keys = (
    enriched
    .select("status", "order_id")
    .subtract(
        df_payments
        .filter(F.col("method") == "promptpay")
        .select("status", "order_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-06-07")))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "amount_max_7d",
        F.least("amount_max_7d", "amount").alias("least_amount_max_7d_amount"),
        F.when(F.col("amount") >= 1500, "High").when(F.col("amount") >= 530, "Medium").otherwise("Low").alias("amount_band"),
    )
)

cleaned = (
    valid
    .filter((F.col("paid_at") >= F.lit("2023-08-08 23:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .withColumn("amount_max_7d_plus_least_amount_max_7d_amount", (F.col("amount_max_7d") + F.col("least_amount_max_7d_amount")))
)

enriched_payments = (
    cleaned
    .filter(F.col("amount") < 1060)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "amount_max_7d",
        F.count("least_amount_max_7d_amount").over(w3).alias("order_id_count_least_amount_max_7d_amount"),
        F.avg("amount_max_7d").over(w4).alias("amount_max_7d_avg_30d"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "amount",
        "paid_at",
        "amount_max_7d",
        "order_id_count_least_amount_max_7d_amount",
        "amount_max_7d_avg_30d",
    )
    .orderBy(F.col("amount_max_7d_avg_30d").asc_nulls_first())
)

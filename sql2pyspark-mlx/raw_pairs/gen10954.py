from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at_month").desc_nulls_last(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("rank_in_group").desc())
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-18 11:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.month("paid_at").alias("paid_at_month"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "rank_in_group")
)

latest_payments = (
    latest
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "rank_in_group", F.col("min_payment_amount"))
    .orderBy(F.col("rank_in_group").desc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("amount", "id")
w2 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").desc())
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_last())

base = (
    df_payments
    .filter(F.col("amount").between(700, 1210) & F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "amount", "paid_at", "status", "latest_rank")
)

ranked = (
    latest
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "amount", "paid_at")
)

result = (
    ranked
    .select("id")
    .orderBy(F.col("id").desc_nulls_last())
)

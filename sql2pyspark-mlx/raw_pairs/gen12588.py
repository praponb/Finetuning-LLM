from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-10-26 05:00:00")))
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM"))
)

enriched = (
    cleaned
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.dense_rank().over(w2).alias("amount_dense_rank_2"),
        F.last("method", ignorenulls=True).over(w3).alias("last_known_method"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    latest
    .filter(F.hour("paid_at").between(7, 14))
    .select(
        "*",
        F.round("amount_dense_rank_2", 2).alias("amount_dense_rank_2_rounded"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "amount_dense_rank",
        "last_known_method",
        "amount_dense_rank_2_rounded",
        "method_clean",
        F.when(F.col("method").rlike("^(promptpay|bank_transfer)$"), "GROUP_B").when(F.col("method").rlike("^(card|paypal)$"), "EXTENDED").otherwise("PRIORITY").alias("method_zone"),
    )
    .orderBy(F.col("amount_dense_rank_2_rounded").desc())
)

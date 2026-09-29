from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("is_promptpay").asc_nulls_first(), "id")
w_hist = Window.partitionBy("order_id").orderBy("is_promptpay", "id")
w4 = Window.partitionBy("order_id").orderBy("seq_no", "id")
w5 = Window.partitionBy("order_id").orderBy("prev_is_promptpay", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-29 00:00:00").cast("timestamp")) & (F.to_date("paid_at") >= "2023-03-26"))
    .select(
        "id",
        "order_id",
        "amount",
        F.initcap(F.trim("method")).alias("method_title"),
        F.when(F.col("method") == "promptpay", 1).otherwise(0).alias("is_promptpay"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    ranked
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isNull())
    .select("*", F.floor("amount").alias("amount_whole"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("amount_whole") >= 72)
    .withColumn("prev_amount", F.lag("amount").over(w_hist))
    .withColumn("next_amount", F.lead("amount").over(w_hist))
    .withColumn("prev_is_promptpay", F.lag("is_promptpay").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

with_windows = (
    metrics
    .select(
        "id",
        "amount",
        "is_promptpay",
        "prev_amount",
        "prev_is_promptpay",
        F.lag("next_amount", 2).over(w4).alias("prev_next_amount"),
        F.first("prev_is_promptpay", ignorenulls=True).over(w5).alias("first_known_prev_is_promptpay"),
    )
)

result = (
    with_windows
    .select("id", "is_promptpay", "prev_next_amount")
    .orderBy(F.col("id").asc_nulls_last())
)

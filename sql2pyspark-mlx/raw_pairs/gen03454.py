from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("discount").desc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("dedup_rank", "id")

valid = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("bank_transfer"))) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.quarter("paid_at").alias("paid_at_quarter"),
        F.concat_ws("/", "method", "status").alias("method_status_key"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.method_status_key",
        F.col("o.status").alias("order_status"),
        F.col("o.discount"),
        F.col("o.amount").alias("order_amount"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest_payments = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_payments
    .select(
        "id",
        "method_status_key",
        "order_status",
        "discount",
        F.when(F.col("order_status").rlike("^(COMPLETED|RETURNED)$"), "GROUP_B").when(F.col("order_status").rlike("^(PENDING|CANCELLED)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("order_status_zone"),
    )
)

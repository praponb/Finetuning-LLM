from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.to_date("paid_at")).orderBy("paid_at", "id").rowsBetween(-3, -1)

ranked = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

cleaned = (
    sessionized
    .filter(F.col("order_id").isNotNull())
    .withColumn("least_row_num_session_no", F.least("row_num", "session_no"))
)

scored = (
    cleaned
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select(
        "*",
        F.lead("least_row_num_session_no").over(w2).alias("next_least_row_num_session_no"),
        F.lead("new_group_flag").over(w2).alias("next_new_group_flag"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("new_group_flag").over(w4).alias("new_group_flag_avg_3_prior"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.*",
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
    )
)

result = (
    enriched
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "new_group_flag",
        "session_no",
        "least_row_num_session_no",
        "next_least_row_num_session_no",
        "next_new_group_flag",
    )
    .orderBy(F.col("id").asc_nulls_last())
)

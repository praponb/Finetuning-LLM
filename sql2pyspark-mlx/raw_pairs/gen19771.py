from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at_hour", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("order_date").desc_nulls_last(), "id")

cleaned = (
    df_payments
    .filter((F.col("amount") > 620) & (F.col("paid_at") > F.lit("2023-11-14 19:00:00").cast("timestamp")))
    .select(
        "id",
        "order_id",
        "status",
        F.hour("paid_at").alias("paid_at_hour"),
        F.substring("method", 1, 3).alias("method_prefix"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

base = (
    cleaned
    .filter(F.col("status") == "REFUNDED")
    .select(
        "id",
        "order_id",
        "status",
        "paid_at_hour",
        "resolved_method",
        F.ceil("paid_at_hour").alias("paid_at_hour_whole"),
        F.length("status").alias("status_length"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.channel"), F.col("d.order_date"), F.col("d.amount"))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "paid_at_hour",
        "resolved_method",
        "paid_at_hour_whole",
        "order_date",
        "amount",
        "row_num",
    )
)

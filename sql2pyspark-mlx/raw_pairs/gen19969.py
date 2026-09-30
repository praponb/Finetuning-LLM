from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.orderBy("order_date", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("order_status")
w4 = Window.partitionBy("order_status").orderBy(F.col("amount").desc())

base = (
    df_payments
    .filter(~F.col("method").eqNullSafe(F.lit("card")))
    .select(
        "*",
        F.when(F.col("amount") >= 1780, "High").when(F.col("amount") >= 410, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled_2"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.amount",
        "a.amount_level",
        "a.method_filled",
        "a.method_filled_2",
        F.col("p.status").alias("order_status"),
        F.col("p.order_date"),
        F.col("p.shipping_fee"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "amount",
        "amount_level",
        "method_filled",
        "order_status",
        "order_date",
    )
)

metrics = (
    deduped
    .filter(F.col("amount") >= 470)
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)

deduped_payments = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped_payments
    .select(
        "id",
        "amount_level",
        "method_filled",
        "order_status",
        "order_date",
        "amount_share",
    )
    .orderBy(F.col("id").desc())
)

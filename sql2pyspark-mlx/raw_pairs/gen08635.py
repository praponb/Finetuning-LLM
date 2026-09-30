from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "*",
        F.date_format("order_date", "yyyy-MM-dd").alias("order_date_label"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

filtered = (
    valid
    .filter((F.col("order_date") > F.lit("2023-02-26").cast("date")) & F.col("discount").between(0.12, 0.37))
    .select("*", (F.col("amount") + F.col("discount")).alias("amount_plus_discount"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") <= F.col("s.order_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_label",
    )
)

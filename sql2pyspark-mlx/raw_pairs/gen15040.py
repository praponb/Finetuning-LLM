from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first(), "id")
w3 = Window.partitionBy("order_status").orderBy("shipping_fee", "id")
w_part = Window.partitionBy(F.year("order_date")).orderBy("amount", "id")
w_hist = Window.partitionBy("order_status").orderBy(F.col("order_amount").desc(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")
w7 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("method") == "bank_transfer"))
    .withColumn("amount_band", F.when(F.col("amount") >= 1230, "High").when(F.col("amount") >= 720, "Medium").otherwise("Low"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "method", "amount", "status", "amount_band")
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    latest
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.discount") <= 0.22)
    .select("a.*", F.col("d.order_date"))
)

enriched = (
    joined
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.status").alias("order_status"),
        F.col("b.shipping_fee"),
        F.col("b.amount").alias("order_amount"),
    )
)

scored = (
    enriched
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 2, 0).over(w3))
    .withColumn("next_order_amount", F.lead("order_amount", 1).over(w_part))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w7))
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_band",
        "order_date",
        "order_status",
        "next_shipping_fee",
    )
)

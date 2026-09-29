from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-09-24").cast("date"))
    .withColumn("shipping_fee_rounded", F.round("shipping_fee", 2))
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("discount").over(w).alias("prev_discount"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("amount_bucket"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") <= 390), "left_semi")
)

remaining_keys = (
    eligible
    .select("status")
    .exceptAll(
        df_orders
        .filter(F.col("amount").between(720, 800))
        .select("status")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"]],
        F.col("dim.city").alias("city"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.amount")).alias("max_amount"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("max_amount").desc_nulls_first())
)

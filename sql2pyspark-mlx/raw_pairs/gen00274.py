from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("discount", "id")
w2 = Window.orderBy(F.col("order_date").desc(), "id")

metrics = (
    df_orders
    .filter(F.col("store_id").isin([3, 5, 12, 23]))
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("amount").over(w).alias("next_amount"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    metrics
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["seq_no_tier", "channel", "seq_no"], ["seq_no_tier", "channel"], []],
        "seq_no_tier",
        "channel",
        "seq_no",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "web", F.col("prev_shipping_fee"))).alias("web_total_prev_shipping_fee"),
        F.sum(F.when(F.col("channel") == "store", F.col("seq_no")).otherwise(0)).alias("store_seq_no"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("store_seq_no") > 0)
    .orderBy(F.col("row_count").asc_nulls_last())
)

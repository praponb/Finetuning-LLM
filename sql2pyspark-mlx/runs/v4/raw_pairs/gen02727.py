from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("price").desc(), "id", "pos")
w_recent = Window.partitionBy("promo").orderBy("qty", "id", "pos")
w3 = Window.partitionBy("promo").orderBy(F.col("product_id").desc(), "id", "pos")
w4 = Window.partitionBy("promo").orderBy(F.col("qty").desc(), "id", "pos")
w_ordered = Window.orderBy(F.col("pos").desc(), "id", "pos")
w6 = Window.partitionBy("promo").orderBy("product_id", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("promo").orderBy(F.col("next_product_id").desc_nulls_last())
w8 = Window.partitionBy("promo").orderBy("prev_pos", "id", "pos")
w9 = Window.partitionBy("promo")
w10 = Window.partitionBy("promo").orderBy("created_at", "id", "pos")
w11 = Window.partitionBy("promo").orderBy("created_at", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

enriched = (
    lines
    .withColumn("price_bucket", F.ntile(3).over(w))
    .withColumn("next_product_id", F.lead("product_id", 1, 0).over(w_recent))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("qty_bucket", F.ntile(3).over(w4))
    .withColumn("prev_pos", F.lag("pos", 2).over(w_recent))
)

enriched_carts = (
    enriched
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no_2"),
        F.sum("prev_pos").over(w6).alias("running_prev_pos"),
        F.rank().over(w_hist).alias("next_product_id_rank"),
    )
)

scored = (
    enriched_carts
    .select(
        "*",
        F.lag("qty", 2).over(w8).alias("prev_qty"),
        F.stddev_samp("seq_no_2").over(w9).alias("promo_stddev_samp_seq_no_2"),
    )
)

flagged = (
    scored
    .withColumn("is_new_session", F.when(F.col("created_at") - F.lag("created_at").over(w10) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w11))

summary = (
    islands
    .groupBy("seq_no")
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.max(F.when(F.col("promo") == "SPRING10", F.col("seq_no"))).alias("spring10_max_seq_no"),
        F.max("created_at").alias("ended_at"),
    )
)

result = (
    summary
    .select("seq_no", "spring10_max_seq_no")
    .orderBy("spring10_max_seq_no", "seq_no")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc())
w2 = Window.partitionBy("customer_id")

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

lines = (
    exploded
    .select("customer_id", "created_at", "qty", F.explode("coupon_codes").alias("code"))
)

enriched = (
    lines
    .withColumn("qty_percent_rank", F.percent_rank().over(w))
    .withColumn("customer_id_avg_qty", F.avg("qty").over(w2))
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("qty_percent_rank_band", F.when(F.col("qty_percent_rank") >= 0.76, "High").when(F.col("qty_percent_rank") >= 0.37, "Medium").otherwise("Low"))
)

result = (
    filtered
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_rows"),
    )
    .orderBy(F.col("vip_rows").desc())
)

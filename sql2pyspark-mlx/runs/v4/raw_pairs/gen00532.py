from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy(F.col("amount").desc(), "id")

scored = (
    df_orders
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("seq_no", F.row_number().over(w2))
)

joined = (
    scored
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.country"))
)

enriched = (
    joined
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.city"), F.col("s.region"), F.col("s.store_name"))
)

result = (
    enriched
    .groupBy("country")
    .agg(
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("amount_per_de"),
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("discount_per_de"),
        F.count_if(F.col("region") == "North").alias("north_events"),
    )
)

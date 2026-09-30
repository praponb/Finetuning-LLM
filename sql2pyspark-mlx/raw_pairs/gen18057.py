from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.discount").alias("order_discount"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

result = (
    with_last
    .withColumn("order_discount_tier", F.when(F.col("order_discount") >= 0.37, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .groupingSets(
        [["order_discount_tier", "country", "week_start"], ["order_discount_tier", "country"]],
        "order_discount_tier",
        "country",
        "week_start",
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("week_start")
)

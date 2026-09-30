from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy(F.date_trunc("WEEK", "order_date"), "customer_id").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("customer_id", "store_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy(F.col("discount").desc(), "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-09-09"))) & (F.col("customer_id") == 14))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

metrics = (
    cleaned
    .filter(F.col("status") == "SHIPPED")
    .withColumn("corr_discount_amount", F.corr("discount", "amount").over(w))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

base = (
    metrics
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-10-02"))) & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.credit_limit"))
)

result = (
    joined
    .withColumn("year_start", F.date_trunc("YEAR", "order_date"))
    .groupingSets([["store_id", "year_start"], ["store_id"]], "store_id", "year_start")
    .agg(
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.sum(F.when(F.col("channel") == "web", F.col("row_seq")).otherwise(0)).alias("web_row_seq"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code", "customer_id").orderBy("credit_limit")
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy("credit_limit")

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

joined = (
    lines
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.credit_limit"), F.col("c.signup_date"))
)

scored = (
    joined
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w))
    .withColumn("credit_limit_sum_2h", F.sum("credit_limit").over(w2))
)

with_windows = (
    scored
    .select(
        "*",
        F.stddev_samp("credit_limit").over(w3).alias("customer_id_stddev_samp_credit_limit"),
        F.avg("credit_limit").over(w4).alias("credit_limit_avg_2h"),
        F.percent_rank().over(w5).alias("credit_limit_percent_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share"),
    )
)

joined_carts = (
    with_windows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.email"), F.col("d.segment"), F.col("d.city"))
)

joined_carts_2 = (
    joined_carts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.city").alias("customer_city"))
)

result = (
    joined_carts_2
    .groupBy(
        F.when(F.col("credit_limit_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("credit_limit_dense_rank_tier"),
        "code",
    )
    .agg(
        F.round(F.sum("credit_limit_share") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("credit_limit_share_per_consumer"),
        F.percentile_approx("credit_limit_percent_rank", [0.25, 0.5, 0.75]).alias("credit_limit_percent_rank_quartiles"),
        F.round(F.sum("credit_limit_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("credit_limit_dense_rank_per_bangkok"),
        F.countDistinct("customer_city").alias("distinct_customer_cities"),
        F.max(F.when(F.col("customer_city") == "Bangkok", F.col("credit_limit_sum_2h"))).alias("bangkok_max_credit_limit_sum_2h"),
    )
    .filter(F.col("bangkok_max_credit_limit_sum_2h") < 0)
)

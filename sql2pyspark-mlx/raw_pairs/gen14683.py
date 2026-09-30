from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.orderBy(F.col("product_id").desc())
w3 = Window.partitionBy("code").orderBy(F.col("product_id").desc())

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

lines = (
    exploded
    .select(
        "customer_id",
        "product_id",
        "promo",
        F.explode("coupon_codes").alias("code"),
    )
)

with_windows = (
    lines
    .select(
        "customer_id",
        "product_id",
        F.stddev_samp("product_id").over(w).alias("code_stddev_samp_product_id"),
        F.rank().over(w2).alias("product_id_rank"),
        F.cume_dist().over(w3).alias("product_id_cume_dist"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.customer_id",
        "s.code_stddev_samp_product_id",
        "s.product_id_rank",
        F.col("c.age"),
    )
)

valid = (
    joined
    .filter(F.col("product_id_rank") <= 4)
    .withColumn("code_stddev_samp_product_id_times_age", (F.col("code_stddev_samp_product_id") * F.col("age")))
)

result = (
    valid
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupingSets(
        [["country", "city"], ["country"]],
        F.col("d.country").alias("country"),
        F.col("d.city").alias("city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.product_id_rank")).alias("avg_product_id_rank"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)

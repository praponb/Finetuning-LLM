from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("segment_length").desc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("segment_length").asc_nulls_first())
w3 = Window.orderBy(F.col("segment_length").desc())

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

joined = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        F.col("d.segment"),
        F.col("d.email"),
        F.col("d.city"),
    )
)

filtered = (
    joined
    .filter(F.col("customer_id").isNotNull() & F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "customer_id",
        "segment",
        "email",
        "city",
        F.length("segment").alias("segment_length"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("segment_length_dense_rank"),
        F.cume_dist().over(w2).alias("segment_length_cume_dist"),
        F.dense_rank().over(w3).alias("segment_length_dense_rank_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "segment",
        "city",
        "segment_length",
        "segment_length_dense_rank",
        "segment_length_cume_dist",
        "segment_length_dense_rank_2",
    )
)

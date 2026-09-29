from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age_minus_credit_limit")
w2 = Window.partitionBy(F.year("created_at"), "segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy(F.col("age_minus_credit_limit").desc())
w_running = Window.partitionBy("segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("segment")

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.is_active"))
    .select("s.*", F.col("d.email"), F.col("d.segment"), F.col("d.age"))
)

joined_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.credit_limit"))
)

with_dims = (
    joined_carts
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.name"))
)

cleaned = (
    with_dims
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (F.col("created_at") < "2024-01-01"))
    .select(
        "id",
        "created_at",
        "email",
        "segment",
        "age",
        "credit_limit",
        "name",
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
    )
)

enriched = (
    cleaned
    .withColumn("age_minus_credit_limit_percent_rank", F.percent_rank().over(w))
    .withColumn("credit_limit_sum_1h", F.sum("credit_limit").over(w2))
    .withColumn("age_minus_credit_limit_dense_rank", F.dense_rank().over(w3))
    .withColumn("credit_limit_sum_2h", F.sum("credit_limit").over(w_running))
)

scored = (
    enriched
    .withColumn("segment_stddev_samp_age_minus_credit_limit_percent_rank", F.stddev_samp("age_minus_credit_limit_percent_rank").over(w5))
)

result = (
    scored
    .select(
        "created_at",
        "email",
        "segment",
        "age",
        "credit_limit",
        "name",
        "age_minus_credit_limit",
        "age_minus_credit_limit_percent_rank",
        "credit_limit_sum_1h",
    )
    .orderBy(F.col("segment").desc_nulls_first(), "age")
)

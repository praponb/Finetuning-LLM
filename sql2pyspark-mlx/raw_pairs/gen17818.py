from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").asc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy("value").orderBy(F.col("credit_limit").asc_nulls_first())
w4 = Window.partitionBy("value").orderBy("credit_limit")
w5 = Window.orderBy(F.col("credit_limit_cume_dist").asc_nulls_first())
w6 = Window.partitionBy("value")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

joined = (
    flattened
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") >= 9000)
    .select("a.customer_id", "a.value", F.col("c.credit_limit"))
)

scored = (
    joined
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w2))
    .withColumn("credit_limit_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("credit_limit_rank", F.rank().over(w4))
)

enriched = (
    scored
    .select(
        "*",
        F.cume_dist().over(w5).alias("credit_limit_cume_dist_cume_dist"),
        F.sum("credit_limit").over(w6).alias("value_sum_credit_limit"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.customer_id",
        "s.value",
        "s.credit_limit",
        "s.credit_limit_rank",
        "s.value_sum_credit_limit",
        F.col("p.email"),
    )
)

kept_keys = (
    with_dims
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isin([11, 22, 28]))
        .select("customer_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("customer_id", "value", "email")
    .orderBy(F.col("customer_id").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_last())
w2 = Window.orderBy("age")
w3 = Window.partitionBy("city")
w4 = Window.orderBy("credit_limit")
w5 = Window.partitionBy("segment")

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

enriched = (
    flattened
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        F.col("b.credit_limit"),
        F.col("b.signup_date"),
        F.col("b.segment"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.credit_limit").between(7900, 16000))
    .select(
        "s.id",
        "s.customer_id",
        "s.credit_limit",
        "s.segment",
        F.col("d.name"),
        F.col("d.city"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.email"), F.col("c.age"))
)

scored = (
    joined
    .select(
        "*",
        F.rank().over(w).alias("age_rank"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "segment",
        "city",
        "age",
        "age_cume_dist",
        F.round((F.col("age_rank") / F.sum("age_rank").over(w3)), 4).alias("age_rank_share"),
        F.rank().over(w4).alias("credit_limit_rank"),
        F.count("credit_limit").over(w5).alias("segment_count_credit_limit"),
        F.min("age").over(w3).alias("city_min_age"),
    )
)

enriched_carts = (
    metrics
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "src.*",
        F.col("p.signup_date"),
        F.col("p.name"),
        F.col("p.age").alias("customer_age"),
    )
)

result = (
    enriched_carts
    .select(
        "customer_id",
        "segment",
        "city",
        "age",
        "age_cume_dist",
        "age_rank_share",
        "credit_limit_rank",
        "city_min_age",
        "signup_date",
    )
    .orderBy("customer_id")
)

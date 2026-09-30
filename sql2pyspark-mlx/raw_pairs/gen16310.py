from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("customer_id").orderBy("age_share")

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

with_dims = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.signup_date") >= "2024-08-29")
    .select(
        "e.id",
        "e.customer_id",
        F.col("p.signup_date"),
        F.col("p.segment"),
        F.col("p.name"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.*",
        F.col("b.age"),
        F.col("b.email"),
        F.col("b.signup_date").alias("customer_signup_date"),
    )
)

with_windows = (
    joined
    .withColumn("customer_id_max_age", F.max("age").over(w))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w2)), 4))
    .withColumn("customer_id_count_age", F.count("age").over(w))
    .withColumn("age_share_2", F.round((F.col("age") / F.sum("age").over(w)), 4))
)

enriched = (
    with_windows
    .select(
        "signup_date",
        "age",
        "email",
        "customer_signup_date",
        "customer_id_max_age",
        "age_share_2",
        F.cume_dist().over(w3).alias("age_share_cume_dist"),
        F.round((F.col("customer_id_max_age") / F.sum("customer_id_max_age").over(w)), 4).alias("customer_id_max_age_share"),
        F.min("age_share_2").over(w).alias("customer_id_min_age_share_2"),
    )
)

result = (
    enriched
    .select(
        "signup_date",
        "age",
        "customer_signup_date",
        "age_share_2",
        "age_share_cume_dist",
        "customer_id_max_age_share",
        "customer_id_min_age_share_2",
    )
    .orderBy(F.col("signup_date").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy(F.col("age").desc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

with_dims = (
    exploded
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.country") == "JP")
    .select(
        "e.customer_id",
        "e.attr_key",
        F.col("p.city"),
        F.col("p.segment"),
        F.col("p.age"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "s.customer_id",
        "s.city",
        "s.age",
        F.col("p.signup_date"),
        F.col("p.country"),
    )
)

metrics = (
    joined
    .select(
        "customer_id",
        "city",
        F.avg("age").over(w).alias("country_avg_age"),
        F.count("age").over(w).alias("country_count_age"),
        F.percent_rank().over(w2).alias("age_percent_rank"),
    )
)

result = (
    metrics
    .select("customer_id", "country_count_age", "age_percent_rank")
    .orderBy(F.col("age_percent_rank").asc_nulls_last(), "country_count_age")
)

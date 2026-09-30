from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_last())

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.is_active"))
    .select("a.*", F.col("p.name"), F.col("p.age"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("src.*", F.col("b.name").alias("customer_name"))
)

base = (
    with_dims
    .filter(F.col("value") != "VIP")
    .select("*", F.length("name").alias("name_length"))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "created_at",
        "name",
        "age",
        F.cume_dist().over(w).alias("age_cume_dist"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy(F.col("age").asc_nulls_first())
w3 = Window.partitionBy("customer_id", "key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.signup_date"))
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.name").like("A%"))
    .select("src.*", F.col("d.city"))
)

enriched_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.country"), F.col("d.age"))
)

result = (
    enriched_carts
    .select(
        "id",
        "coupon_codes",
        "signup_date",
        "city",
        "country",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.percent_rank().over(w2).alias("age_percent_rank"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share_2"),
    )
)

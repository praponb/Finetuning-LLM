from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

metrics = (
    exploded
    .select(
        "id",
        "customer_id",
        "qty",
        F.max("product_id").over(w).alias("customer_id_max_product_id"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.customer_id_max_product_id",
        F.col("c.age"),
        F.col("c.name"),
        F.col("c.credit_limit"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") < "2024-07-01")
    .select(
        "a.id",
        "a.customer_id",
        "a.customer_id_max_product_id",
        "a.credit_limit",
        F.col("c.signup_date"),
        F.col("c.email"),
    )
)

result = (
    enriched
    .select("id", "customer_id", "customer_id_max_product_id", "credit_limit")
    .orderBy(F.col("customer_id_max_product_id").desc())
)

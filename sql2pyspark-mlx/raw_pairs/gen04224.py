from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city", "customer_id").orderBy(F.col("product_id").desc())
w3 = Window.orderBy(F.col("product_id").desc())
w4 = Window.orderBy(F.col("country_count_product_id").desc_nulls_first())
w5 = Window.partitionBy("country", "city")

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", "coupon_codes", F.col("line.product_id"))
)

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.is_active"))
    .select(
        "src.customer_id",
        "src.product_id",
        F.col("d.segment"),
        F.col("d.city"),
        F.col("d.signup_date"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.customer_id",
        "a.product_id",
        "a.city",
        "a.signup_date",
        F.col("p.email"),
        F.col("p.signup_date").alias("customer_signup_date"),
        F.col("p.country"),
    )
)

with_windows = (
    joined
    .withColumn("country_count_product_id", F.count("product_id").over(w))
    .withColumn("product_id_rank", F.rank().over(w2))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w3))
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.customer_id",
        "e.product_id",
        "e.city",
        "e.email",
        "e.country",
        "e.country_count_product_id",
        F.col("p.country").alias("customer_country"),
        F.col("p.name"),
    )
)

with_windows_carts = (
    with_dims
    .select(
        "email",
        "country_count_product_id",
        "customer_country",
        "name",
        F.percent_rank().over(w4).alias("country_count_product_id_percent_rank"),
        F.round((F.col("product_id") / F.sum("product_id").over(w5)), 4).alias("product_id_share"),
    )
)

result = (
    with_windows_carts
    .groupBy("customer_country")
    .agg(
        F.count(F.when(F.col("customer_country") == "JP", 1)).alias("jp_rows"),
        F.count(F.when(F.col("customer_country") == "DE", 1)).alias("de_rows"),
        F.count_if(F.col("customer_country") == "US").alias("us_events"),
    )
    .filter(F.col("jp_rows") > 1)
)

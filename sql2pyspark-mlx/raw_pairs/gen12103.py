from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.year("created_at"), "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_last())

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

scored = (
    lines
    .withColumn("product_id_max_15m", F.max("product_id").over(w))
    .withColumn("price_avg_30m", F.avg("price").over(w2))
    .withColumn("product_id_percent_rank", F.percent_rank().over(w3))
)

with_dims = (
    scored
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.country") == "UK")
    .select("a.*", F.col("d.credit_limit"))
)

with_dims_carts = (
    with_dims
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"))
)

with_dims_carts_2 = (
    with_dims_carts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.*", F.col("p.email"))
)

result = (
    with_dims_carts_2
    .cube("customer_id", "promo")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("price_per_new_york"),
        F.max("created_at").alias("ended_at"),
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
    )
    .filter(F.col("price_per_new_york") > 0)
    .orderBy(F.col("promo").desc())
)

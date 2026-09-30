from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("price").asc_nulls_last(), "id", "line_no")
w2 = Window.partitionBy("customer_id", "promo").orderBy(F.col("line_no").desc(), "id", "line_no")
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty_per_price").desc())

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

latest = (
    lines
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    ranked
    .filter(F.col("promo").isNotNull())
    .withColumn("qty_per_price", F.round((F.col("qty") / F.col("price")), 2))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("DIRECT")))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country").isin(["DE", "UK"]))
    .select("s.*", F.col("b.email"))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.email").alias("customer_email"), F.col("b.credit_limit"))
)

latest_carts = (
    with_dims
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_carts
    .select(
        "id",
        "line_no",
        "product_id",
        "qty",
        "price",
        "promo",
        "row_num",
        "resolved_promo",
        "email",
    )
    .orderBy(F.col("line_no").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_whole", F.ceil("rating"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "rating", "rating_whole")
)

with_last = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

kept_keys = (
    with_last
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("country").isin(["US", "JP"]))
        .select("id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("rating", "rating_whole", "product_launch_date")
    .orderBy("rating_whole", F.col("rating").desc())
)

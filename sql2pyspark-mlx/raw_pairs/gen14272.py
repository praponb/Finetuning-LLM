from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date")

with_dims = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.stock_qty") != 378)
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        F.col("b.cost"),
    )
)

latest = (
    with_dims
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims_store_sales = (
    latest
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.id", "s.product_id", F.col("b.product_name"))
)

filtered = (
    with_dims_store_sales
    .filter(~F.col("product_name").like("wi%"))
    .withColumn("product_name_length", F.length("product_name"))
)

result = (
    filtered
    .select("id", "product_name_length")
    .orderBy(F.col("id").desc_nulls_last())
)

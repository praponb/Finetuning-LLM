from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("unit_price")
w3 = Window.partitionBy("product_id").orderBy(F.col("launch_date").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round((F.col("quantity") * 100), 2).alias("quantity_adjusted"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "product_id",
        "quantity",
        F.row_number().over(w).alias("row_seq"),
        F.percent_rank().over(w2).alias("unit_price_percent_rank"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.row_seq",
        F.col("b.stock_qty"),
        F.col("b.launch_date"),
    )
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    ranked
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.product_name"), F.col("p.brand"), F.col("p.category"))
)

with_dims_order_items = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.*", F.col("p.brand").alias("product_brand"), F.col("p.price"))
)

result = (
    with_dims_order_items
    .select("id", "row_seq", "launch_date", "brand", "product_brand", "price")
)

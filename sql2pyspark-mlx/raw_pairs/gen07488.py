from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("quantity") >= 6)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.product_name"), F.col("b.cost"))
)

filtered = (
    joined
    .filter(F.col("product_id").isNotNull() & F.col("line_total").between(23, 64))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "product_name",
        (F.col("quantity") * F.col("unit_price")).alias("line_total_2"),
    )
)

joined_order_items = (
    filtered
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty") > 213)
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.unit_price",
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
)

enriched = (
    joined_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.price"), F.col("p.product_name"))
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "quantity", "product_name")
)

cleaned_order_items = (
    latest
    .filter(F.col("quantity") != 3)
    .select("*", F.regexp_replace("product_name", " ", "_").alias("product_name_slug"))
)

result = (
    cleaned_order_items
    .select("id", "quantity", "product_name")
    .orderBy("quantity")
)

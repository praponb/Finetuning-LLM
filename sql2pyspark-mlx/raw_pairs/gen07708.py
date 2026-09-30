from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price_int").desc_nulls_last(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.category"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.unit_price_int",
        "a.latest_rank",
        "a.cost",
        "a.category",
        F.col("p.stock_qty"),
    )
)

totals = (
    enriched
    .groupBy("product_id")
    .agg(
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") > 0)
)

result = (
    totals
    .select("product_id", "sports_events", "home_events")
    .orderBy("product_id")
)

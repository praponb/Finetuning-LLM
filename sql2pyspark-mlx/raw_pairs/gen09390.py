from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand")
w2 = Window.orderBy("price", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-12").cast("date")) & F.col("brand").isNotNull())
    .withColumn("cost_plus_stock_qty", (F.col("cost") + F.col("stock_qty")))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

with_dims = (
    valid
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.price",
        "src.stock_qty",
        "src.launch_date",
        "src.tags",
        "src.supplier_id",
        "src.cost_plus_stock_qty",
        "src.brand_filled",
        F.col("p.rating"),
    )
)

filtered = (
    with_dims
    .filter(~F.col("brand").eqNullSafe(F.lit("Globex")))
    .select(
        "*",
        F.when(F.col("price") >= 1010, "High").when(F.col("price") >= 390, "Medium").otherwise("Low").alias("price_tier"),
    )
)

scored = (
    filtered
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("prev_price", F.lag("price", 1, 0).over(w2))
)

joined = (
    scored
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.rating").alias("supplier_rating"))
)

joined_products = (
    joined
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.price",
        "a.launch_date",
        "a.supplier_id",
        "a.cost_plus_stock_qty",
        "a.rating",
        "a.rating_share",
        "a.prev_price",
        F.col("b.city"),
        F.col("b.supplier_name"),
    )
)

kept = (
    joined_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([3, 7, 9, 10])), "left_semi")
)

result = (
    kept
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "launch_date",
        "supplier_id",
        "cost_plus_stock_qty",
        "rating",
        "city",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
        F.col("min_product_cost"),
    )
    .orderBy(F.col("cost_plus_stock_qty").desc())
)

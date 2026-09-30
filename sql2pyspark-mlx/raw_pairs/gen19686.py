from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.orderBy("store_sale_sale_date", "id")
w3 = Window.partitionBy("brand").orderBy("store_sale_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("launch_date"), "city").orderBy(F.col("price").desc(), "id")

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.revenue"),
    )
)

base = (
    latest_match
    .filter(F.col("brand").isNotNull())
    .withColumn("price_rounded", F.round("price", 1))
)

joined = (
    base
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "inner")
    .filter(F.upper(F.col("b.supplier_name")).like("%OKY%"))
    .select(
        "a.id",
        "a.product_name",
        "a.brand",
        "a.price",
        "a.stock_qty",
        "a.launch_date",
        "a.supplier_id",
        "a.store_sale_sale_date",
        F.col("b.country"),
        F.col("b.city"),
        F.col("b.rating"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.country").alias("supplier_country"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
        F.last("supplier_country", ignorenulls=True).over(w3).alias("last_known_supplier_country"),
        F.ntile(3).over(w4).alias("price_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("brand").isNotNull() & (F.col("rating") < 2.6))
    .select(
        "*",
        (F.col("price_bucket") + F.col("stock_qty")).alias("price_bucket_plus_stock_qty"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.store_id").isin([2, 3, 4])), "left_anti")
)

result = (
    kept
    .groupBy("city")
    .pivot("country", ["DE", "US"])
    .agg(F.max("price"))
)

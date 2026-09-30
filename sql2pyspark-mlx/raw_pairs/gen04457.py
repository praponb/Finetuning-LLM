from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "brand")
w2 = Window.partitionBy("supplier_id").orderBy("is_books", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-06-22")))
    .select("*", F.when(F.col("category") == "Books", 1).otherwise(0).alias("is_books"))
)

joined = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.country"), F.col("s.city"), F.col("s.supplier_name"))
)

kept = (
    joined
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id") == 5), "left_anti")
)

metrics = (
    kept
    .withColumn("corr_price_cost", F.corr("price", "cost").over(w))
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.brand",
        "a.price",
        "a.stock_qty",
        "a.tags",
        "a.supplier_id",
        "a.supplier_name",
        "a.corr_price_cost",
        F.col("d.country").alias("supplier_country"),
    )
)

result = with_dims.select("id", "category", "brand", "price", "stock_qty", "supplier_name")

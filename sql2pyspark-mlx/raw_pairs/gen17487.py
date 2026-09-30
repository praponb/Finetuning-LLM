from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")

cleaned = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .select(
        "*",
        F.floor("cost").alias("cost_whole"),
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.price",
        "a.stock_qty",
        "a.tags",
        "a.supplier_id",
        "a.cost_whole",
        "a.days_since_launch_date",
        "a.brand_filled",
        "a.dedup_rank",
        F.col("p.supplier_name"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "src.id",
        "src.category",
        "src.stock_qty",
        "src.tags",
        "src.supplier_id",
        "src.dedup_rank",
        "src.supplier_name",
        F.col("b.city"),
        F.col("b.supplier_name").alias("supplier_supplier_name"),
    )
)

kept = (
    with_dims
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 10])), "left_anti")
)

with_dims_products = (
    kept
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.supplier_name").alias("supplier_supplier_name_2"))
)

latest = (
    with_dims_products
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy("supplier_id")
    .pivot("category", ["Grocery", "Fashion"])
    .agg(F.sum("stock_qty"))
)

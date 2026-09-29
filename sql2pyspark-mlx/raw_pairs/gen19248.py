from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "launch_date")).orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("stock_qty_scaled", F.round((F.col("stock_qty") * 100), 2))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.cost",
        "src.launch_date",
        "src.tags",
        "src.supplier_id",
        "src.stock_qty_scaled",
        F.col("d.city"),
        F.col("d.supplier_name"),
    )
)

metrics = (
    enriched
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "cost",
        "launch_date",
        "supplier_id",
        "stock_qty_scaled",
        "city",
        F.avg("cost").over(w).alias("cost_avg_3"),
        F.avg("stock_qty_scaled").over(w2).alias("stock_qty_scaled_avg_2"),
        F.last("stock_qty_scaled", ignorenulls=True).over(w3).alias("last_known_stock_qty_scaled"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([12, 14, 23, 33])), "left_anti")
)

base = (
    kept
    .filter(F.col("launch_date") > F.lit("2023-05-10").cast("date"))
    .select(
        "id",
        "launch_date",
        "supplier_id",
        "stock_qty_scaled",
        "city",
        "cost_avg_3",
        "last_known_stock_qty_scaled",
        (F.col("stock_qty_scaled") / F.nullif("last_known_stock_qty_scaled", F.lit(0))).alias("stock_qty_scaled_per_last_known_stock_qty_scaled"),
    )
)

result = (
    base
    .alias("e")
    .join(df_suppliers.alias("dim"), F.col("e.supplier_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["supplier_city", "product_city"], ["supplier_city"], []],
        F.col("dim.city").alias("supplier_city"),
        F.col("e.city").alias("product_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.last_known_stock_qty_scaled")).alias("min_last_known_stock_qty_scaled"),
    )
)

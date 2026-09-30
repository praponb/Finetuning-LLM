from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([4, 13, 23]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 340, "High").when(F.col("unit_price") >= 80, "Medium").otherwise("Low").alias("unit_price_band"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.unit_price_band",
        "s.line_total",
        F.col("d.brand"),
    )
)

enriched_order_items = (
    enriched
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .filter(F.year(F.col("b.launch_date")) >= 2024)
    .select(
        "s.id",
        "s.product_id",
        "s.line_total",
        "s.brand",
        F.col("b.cost"),
        F.col("b.brand").alias("product_brand"),
        F.col("b.stock_qty"),
    )
)

base = (
    enriched_order_items
    .filter(F.col("product_brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Globex"))))
    .select(
        "*",
        F.round((F.col("cost") * 1.07), 2).alias("cost_adjusted"),
        F.coalesce("product_brand", F.lit("N/A")).alias("resolved_product_brand"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)

result = (
    metrics
    .groupBy("product_brand")
    .agg(
        F.count(F.when(F.col("product_brand") == "Acme", 1)).alias("acme_rows"),
        F.percentile_approx("cost_adjusted", [0.25, 0.5, 0.75]).alias("cost_adjusted_quartiles"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
)

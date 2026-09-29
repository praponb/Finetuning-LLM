from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_times_cost", "id")
w2 = Window.partitionBy("product_id", "brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("quantity") >= 234)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
        F.floor("quantity").alias("quantity_whole_2"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.quantity_whole",
        F.col("d.category"),
        F.col("d.cost"),
        F.col("d.stock_qty"),
    )
)

base = (
    with_dims
    .filter(F.col("category").isNotNull() & (F.col("quantity_whole") > 62))
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_whole",
        "stock_qty",
        (F.col("quantity") * F.col("cost")).alias("quantity_times_cost"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.brand"))
)

base_inventory = (
    enriched
    .filter((~F.col("brand").eqNullSafe(F.lit("Globex"))) & (F.col("brand") == "Acme"))
    .select(
        "*",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("quantity_times_cost", F.lit(0)).alias("quantity_times_cost_filled"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

metrics = (
    base_inventory
    .select(
        "id",
        "product_id",
        "quantity_whole",
        "stock_qty",
        "brand",
        "brand_filled",
        F.lead("quantity_times_cost_filled", 1).over(w).alias("next_quantity_times_cost_filled"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
    )
)

result = (
    metrics
    .select(
        "id",
        "quantity_whole",
        "stock_qty",
        "brand",
        "brand_filled",
        "cumulative_quantity",
        F.when(F.col("brand").rlike("^(Acme|Globex)$"), "PRIORITY").otherwise("CORE").alias("brand_zone"),
    )
    .orderBy(F.col("quantity_whole").desc_nulls_first(), "stock_qty")
)

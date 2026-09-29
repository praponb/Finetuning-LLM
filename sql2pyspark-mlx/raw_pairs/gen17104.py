from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id")

with_windows = (
    df_products
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("cumulative_stock_qty"),
        F.corr("cost", "price").over(w2).alias("corr_cost_price"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_anti")
)

matched_keys = (
    eligible
    .select("supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("category") != "Books")
        .select("supplier_id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("launch_date") > F.lit("2023-09-10").cast("date")) & F.col("brand").isNotNull())
    .withColumn("sorted_tags", F.sort_array("tags"))
    .withColumn("brand_upper", F.upper("brand"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("OTHER")))
)

joined = (
    cleaned
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select("a.*", F.col("p.supplier_name"), F.col("p.city"))
)

result = (
    joined
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("corr_cost_price")).otherwise(0)).alias("umbrella_corr_cost_price"),
        F.count_if(F.col("city") == "London").alias("london_events"),
    )
    .orderBy("books_count")
)

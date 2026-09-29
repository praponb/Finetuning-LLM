from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter(F.col("brand") == "Acme")
    .select(
        "id",
        "category",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("stock_qty") * F.col("cost")).alias("stock_qty_times_cost"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

enriched = (
    base
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w))
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
)

matched_keys = (
    enriched
    .select("supplier_id", "category")
    .intersect(
        df_products
        .filter(F.col("launch_date") > "2023-04-01")
        .select("supplier_id", "category")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube("supplier_id")
    .agg(
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.percentile_approx("cumulative_cost", [0.25, 0.5, 0.75]).alias("cumulative_cost_quartiles"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .orderBy(F.col("supplier_id").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_rounded").desc(), "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-03-17").cast("date")) & F.col("product_id").isin([4, 5, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

metrics = (
    filtered
    .filter(F.col("last_restock") < "2024-02-05")
    .select(
        "id",
        "store_id",
        "product_id",
        F.sum("quantity_rounded").over(w).alias("running_sum_quantity_rounded"),
        F.count("quantity_rounded").over(w).alias("running_count_quantity_rounded"),
        F.max("quantity_rounded").over(w).alias("running_max_quantity_rounded"),
        F.avg("quantity_rounded").over(w).alias("running_avg_quantity_rounded"),
        F.ntile(3).over(w2).alias("quantity_rounded_bucket"),
    )
)

remaining_keys = (
    metrics
    .select("product_id")
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([2, 3, 8, 10]))
        .select("product_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("running_sum_quantity_rounded") < 300)
    .select(
        "id",
        "store_id",
        "running_sum_quantity_rounded",
        "running_count_quantity_rounded",
        "running_max_quantity_rounded",
        "running_avg_quantity_rounded",
        "quantity_rounded_bucket",
        F.col("running_avg_quantity_rounded").cast("int").alias("running_avg_quantity_rounded_int"),
    )
)

result = (
    base
    .cube(
        F.when(F.col("running_count_quantity_rounded") >= 16, "PREMIUM").otherwise("STANDARD").alias("running_count_quantity_rounded_tier"),
        "store_id",
    )
    .agg(
        F.min("running_count_quantity_rounded").alias("min_running_count_quantity_rounded"),
        F.round(F.avg("running_count_quantity_rounded"), 2).alias("avg_running_count_quantity_rounded"),
        F.percentile_approx("running_sum_quantity_rounded", 0.95).alias("running_sum_quantity_rounded_p95"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("running_sum_quantity_rounded_p95") >= 0)
)

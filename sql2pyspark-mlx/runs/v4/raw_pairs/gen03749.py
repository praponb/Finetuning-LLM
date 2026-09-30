from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w_ordered = Window.partitionBy("supplier_id").orderBy(F.col("days_since_launch_date").asc_nulls_first())
w3 = Window.partitionBy("category").orderBy("stock_qty_num")
w_part = Window.orderBy(F.col("price_per_stock_qty").asc_nulls_first())

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-22").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_products
        .filter(F.col("price") <= 640)
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("launch_date") >= F.lit("2023-09-04").cast("date")) & F.col("supplier_id").isNotNull() & F.col("stock_qty").between(189, 233))
    .select(
        "*",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.col("stock_qty").cast("double").alias("stock_qty_num"),
    )
)

scored = (
    base
    .withColumn("brand_max_price", F.max("price").over(w))
    .withColumn("days_since_launch_date_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("stock_qty_num_rank", F.rank().over(w3))
    .withColumn("price_per_stock_qty_rank", F.rank().over(w_part))
)

result = (
    scored
    .select("category", "price", "stock_qty_num")
    .unpivot(["category"], ["price", "stock_qty_num"], "metric_name", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)

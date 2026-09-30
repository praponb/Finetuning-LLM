from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("category").orderBy("cost_int", "id")
w_grp = Window.partitionBy("category").orderBy(F.col("stock_qty").asc_nulls_first())
w4 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-16").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("launch_date_plus_months", F.add_months("launch_date", 3))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("NONE")))
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "dedup_rank",
        F.col("cost").cast("int").alias("cost_int"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

metrics = (
    filtered
    .withColumn("cost_int_bucket", F.ntile(3).over(w2))
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("stock_qty_cume_dist", F.cume_dist().over(w4))
)

result = (
    metrics
    .withColumn("year_start", F.date_trunc("YEAR", "launch_date"))
    .groupingSets(
        [["category", "year_start"], ["category"], []],
        "category",
        "year_start",
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
        F.count("*").alias("event_count"),
    )
)

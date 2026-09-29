from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-11-11").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.least("revenue", "units").alias("least_revenue_units"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.stock_qty"))
)

filtered = (
    enriched
    .filter(F.col("sale_date") > F.lit("2023-11-30").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_int",
        "dedup_rank",
        (F.col("revenue") * F.col("stock_qty")).alias("revenue_times_stock_qty"),
        (F.col("least_revenue_units") - F.col("dedup_rank")).alias("least_revenue_units_minus_dedup_rank"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("revenue_int") >= 69, "HIGH").otherwise("LOW").alias("revenue_int_tier"),
    )
    .agg(
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
    )
    .orderBy(F.col("revenue_p95").desc_nulls_first())
)

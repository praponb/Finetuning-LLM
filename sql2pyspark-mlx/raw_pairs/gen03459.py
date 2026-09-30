from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "product_id").orderBy(F.col("units").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("running_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-30").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
        F.date_format("sale_date", "yyyy-MM-dd").alias("sale_date_label"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.price").between(410, 500))
    .select(
        "s.id",
        "s.product_id",
        "s.units",
        "s.revenue",
        "s.revenue_per_unit",
        F.col("b.cost"),
        F.col("b.price"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.units",
        "s.revenue_per_unit",
        "s.cost",
        F.col("p.brand"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.ntile(4).over(w2).alias("units_bucket"),
        F.sum("cost").over(w3).alias("running_cost"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.avg("revenue_per_unit").over(w4).alias("running_avg_revenue_per_unit"),
        F.sum("revenue_per_unit").over(w4).alias("running_sum_revenue_per_unit"),
        F.min("revenue_per_unit").over(w4).alias("running_min_revenue_per_unit"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w_hist = Window.partitionBy("product_id").orderBy("adjusted_cumulative_revenue_per_unit", "id")
w5 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.max("quantity").alias("max_order_item_quantity"))

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-12").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.first("revenue", ignorenulls=True).over(w_recent).alias("first_known_revenue"),
        F.first("revenue_per_unit", ignorenulls=True).over(w_recent).alias("first_known_revenue_per_unit"),
        F.sum("revenue_per_unit").over(w2).alias("cumulative_revenue_per_unit"),
    )
)

base = (
    with_windows
    .filter(F.col("sale_date") > F.lit("2023-04-07").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue_per_unit",
        "cumulative_revenue_per_unit",
        F.round((F.col("cumulative_revenue_per_unit") * 0.85), 2).alias("adjusted_cumulative_revenue_per_unit"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .withColumn("adjusted_cumulative_revenue_per_unit_bucket", F.ntile(3).over(w_hist))
    .withColumn("prev_adjusted_cumulative_revenue_per_unit", F.lag("adjusted_cumulative_revenue_per_unit", 1, 0).over(w5))
)

result = (
    enriched
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue_per_unit",
        "cumulative_revenue_per_unit",
        "adjusted_cumulative_revenue_per_unit",
        "adjusted_cumulative_revenue_per_unit_bucket",
        "prev_adjusted_cumulative_revenue_per_unit",
        F.col("max_order_item_quantity"),
    )
)

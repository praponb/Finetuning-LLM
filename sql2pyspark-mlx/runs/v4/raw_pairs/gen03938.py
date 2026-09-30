from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("sale_date_period", "id")
w4 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id", "product_id").orderBy("last_known_revenue_per_unit", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("last_known_revenue_per_unit").desc(), "id")
w_ordered = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(-5, -1)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-07-18"))) & F.col("sale_date").between("2024-10-23", "2024-12-01"))
    .select(
        "*",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
        F.date_trunc("YEAR", "sale_date").alias("sale_date_period"),
    )
)

with_windows = (
    cleaned
    .withColumn("last_known_revenue_per_unit", F.last("revenue_per_unit", ignorenulls=True).over(w))
    .withColumn("cumulative_revenue_per_unit", F.sum("revenue_per_unit").over(w2))
    .withColumn("next_units", F.lead("units", 1).over(w3))
)

with_windows_store_sales = (
    with_windows
    .select(
        "*",
        F.avg("revenue").over(w4).alias("revenue_avg_3_prior"),
        F.last("last_known_revenue_per_unit", ignorenulls=True).over(w5).alias("last_known_last_known_revenue_per_unit"),
        F.avg("revenue_per_unit").over(w6).alias("revenue_per_unit_avg_3"),
        F.row_number().over(w7).alias("event_seq"),
        F.avg("cumulative_revenue_per_unit").over(w_ordered).alias("cumulative_revenue_per_unit_avg_5_prior"),
    )
)

result = (
    with_windows_store_sales
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_per_unit",
        "sale_date_period",
        "cumulative_revenue_per_unit",
        F.col("min_inventory_quantity"),
    )
    .orderBy(
        F.col("cumulative_revenue_per_unit").desc_nulls_first(),
        "sale_date_period",
    )
)

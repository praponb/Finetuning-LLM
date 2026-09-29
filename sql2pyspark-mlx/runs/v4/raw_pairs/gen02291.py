from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("greatest_revenue_units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("revenue_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("last_known_revenue").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("last_known_revenue", "id")
w6 = Window.partitionBy("product_id").orderBy("revenue_share", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("product_id").orderBy("greatest_revenue_units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("last_known_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy(F.col("last_known_revenue_avg_6_prior").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy(F.col("next_revenue_share").desc())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

valid = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)

valid_store_sales = (
    valid
    .filter((F.col("sale_date") >= F.lit("2024-03-30").cast("date")) & F.col("product_id").isin([5, 6, 10]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.coalesce("greatest_revenue_units", F.lit(0)).alias("greatest_revenue_units_filled"),
    )
)

with_windows = (
    valid_store_sales
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
)

enriched = (
    with_windows
    .withColumn("running_greatest_revenue_units_filled", F.sum("greatest_revenue_units_filled").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

enriched_store_sales = (
    enriched
    .withColumn("next_row_seq", F.lead("row_seq", 1).over(w5))
    .withColumn("last_known_revenue_avg_6_prior", F.avg("last_known_revenue").over(w6))
    .withColumn("next_revenue_share", F.lead("revenue_share", 2, 0).over(w5))
    .withColumn("running_revenue_share", F.sum("revenue_share").over(w7))
    .withColumn("last_known_last_known_revenue", F.last("last_known_revenue", ignorenulls=True).over(w8))
)

with_windows_store_sales = (
    enriched_store_sales
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("next_revenue_share_dense_rank", F.dense_rank().over(w_running))
)

result = (
    with_windows_store_sales
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_share",
        "row_seq",
        "last_known_revenue_avg_6_prior",
        "next_revenue_share",
        "running_revenue_share",
        F.col("sum_inventory_quantity"),
    )
    .orderBy("row_seq", "sale_date")
)

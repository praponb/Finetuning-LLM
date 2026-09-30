from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("days_since_sale_date").desc(), "id")
w2 = Window.partitionBy(F.month("sale_date_plus_63d"), "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy("days_since_sale_date", "id")
w5 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-07-01")))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.date_add("sale_date", 63).alias("sale_date_plus_63d"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    ranked
    .select(
        "*",
        F.sum("days_since_sale_date").over(w2).alias("running_days_since_sale_date"),
        F.avg("days_since_sale_date").over(w3).alias("days_since_sale_date_avg_3"),
        F.ntile(5).over(w4).alias("days_since_sale_date_bucket"),
        F.sum("days_since_sale_date").over(w5).alias("running_days_since_sale_date_2"),
        F.sum("days_since_sale_date").over(w6).alias("running_days_since_sale_date_3"),
    )
)

cleaned = (
    scored
    .filter(F.col("sale_date") > F.lit("2023-09-05").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "sale_date_plus_63d",
        "days_since_sale_date",
        "days_since_sale_date_avg_3",
        F.round((F.col("running_days_since_sale_date_3") / F.col("days_since_sale_date_bucket")), 2).alias("running_days_since_sale_date_3_per_days_since_sale_date_bucket"),
    )
)

result = (
    cleaned
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "sale_date_plus_63d",
        "days_since_sale_date",
        "days_since_sale_date_avg_3",
        "running_days_since_sale_date_3_per_days_since_sale_date_bucket",
        F.col("min_inventory_quantity"),
    )
)

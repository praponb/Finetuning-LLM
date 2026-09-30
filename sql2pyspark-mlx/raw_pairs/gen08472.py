from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date_month_end", "id")
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("week", "sale_date_month_end"), "product_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("sale_date")
w5 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-30").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.when(F.col("units") >= 41, "High").when(F.col("units") >= 7, "Medium").otherwise("Low").alias("units_tier"),
    )
)

with_windows = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.sale_date",
        "s.units",
        "s.revenue",
        "s.row_seq",
        "s.event_seq",
        F.col("d.price"),
        F.col("d.cost"),
        F.col("d.product_name"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned_store_sales = (
    latest
    .filter(F.col("event_seq") >= 6)
    .select("*", F.round((F.col("revenue") * 1.1), 2).alias("revenue_adjusted"))
)

metrics = (
    cleaned_store_sales
    .select(
        "*",
        F.sum("units").over(w2).alias("running_sum_units"),
        F.max("units").over(w2).alias("running_max_units"),
        F.count("units").over(w2).alias("running_count_units"),
        F.avg("units").over(w2).alias("running_avg_units"),
        F.sum("price").over(w5).alias("cumulative_price"),
    )
)

result = (
    metrics
    .select(
        "id",
        "units",
        "revenue",
        "event_seq",
        "product_name",
        "revenue_adjusted",
        "running_sum_units",
        "running_max_units",
        "cumulative_price",
        F.when(F.col("product_name").rlike("^(Running Shoes|Yoga Mat)$"), "EXTENDED").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "GROUP_B").otherwise("CORE").alias("product_name_zone"),
    )
    .orderBy(F.col("running_sum_units").desc())
)

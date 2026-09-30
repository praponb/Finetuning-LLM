from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy(F.col("prev_units").desc_nulls_first())
w6 = Window.partitionBy("store_id", "category").orderBy(F.col("dedup_rank").desc())
w7 = Window.partitionBy("store_id").orderBy(F.col("units_avg_2").desc(), "id")
w8 = Window.partitionBy("store_id").orderBy(F.col("grocery_units_avg_3").desc())

with_windows = (
    df_store_sales
    .select(
        "*",
        F.lag("units", 1).over(w).alias("prev_units"),
        F.last("revenue", ignorenulls=True).over(w2).alias("last_known_revenue"),
        F.avg("units").over(w3).alias("units_avg_3"),
        F.avg("units").over(w4).alias("units_avg_2"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.brand").isin(["Globex", "Stark", "Initech"]))
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.prev_units",
        "a.units_avg_3",
        "a.units_avg_2",
        F.col("d.category"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

latest_store_sales = (
    latest
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

latest_store_sales_2 = (
    latest_store_sales
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest_store_sales_2
    .groupBy("store_id", "category")
    .agg(
        F.count("*").alias("event_count"),
        F.avg("units_avg_2").alias("avg_units_avg_2"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("units_avg_3")).otherwise(0)).alias("grocery_units_avg_3"),
    )
    .withColumn("grocery_units_avg_3_percent_rank", F.percent_rank().over(w8))
)

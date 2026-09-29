from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("product_id").orderBy(F.col("running_avg_last_restock_quarter").desc())

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-08-22")
    .select(
        "id",
        "product_id",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

valid = (
    cleaned
    .filter(F.col("last_restock") <= "2024-10-01")
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM-dd"))
)

valid_inventory = (
    valid
    .filter((F.col("last_restock") > F.lit("2023-06-12").cast("date")) & (F.col("last_restock_day") >= 56))
    .select(
        "id",
        "product_id",
        "last_restock_day",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

enriched = (
    valid_inventory
    .filter(F.col("product_id").isin([3, 5]))
    .withColumn("running_min_last_restock_quarter", F.min("last_restock_quarter").over(w))
    .withColumn("running_sum_last_restock_quarter", F.sum("last_restock_quarter").over(w))
    .withColumn("running_count_last_restock_quarter", F.count("last_restock_quarter").over(w))
    .withColumn("running_avg_last_restock_quarter", F.avg("last_restock_quarter").over(w))
)

enriched_inventory = (
    enriched
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.last_restock_day",
        "src.last_restock_quarter",
        "src.running_min_last_restock_quarter",
        "src.running_sum_last_restock_quarter",
        "src.running_avg_last_restock_quarter",
        F.col("d.category"),
    )
)

scored = (
    enriched_inventory
    .withColumn("running_count_running_sum_last_restock_quarter", F.count("running_sum_last_restock_quarter").over(w))
    .withColumn("running_max_running_sum_last_restock_quarter", F.max("running_sum_last_restock_quarter").over(w))
    .withColumn("running_min_running_sum_last_restock_quarter", F.min("running_sum_last_restock_quarter").over(w))
    .withColumn("running_avg_last_restock_quarter_rank", F.rank().over(w_ordered_2))
)

result = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.brand").alias("brand"),
        F.col("e.category").alias("inventory_category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.running_min_last_restock_quarter")).alias("min_running_min_last_restock_quarter"),
        F.min(F.col("e.running_sum_last_restock_quarter")).alias("min_running_sum_last_restock_quarter"),
    )
)

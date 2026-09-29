from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("last_known_quantity", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-27").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 204, "High").when(F.col("quantity") >= 117, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

metrics = (
    valid
    .filter(F.col("quantity") == 142)
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("last_known_quantity_2", F.last("quantity", ignorenulls=True).over(w))
)

joined = (
    metrics
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.brand"), F.col("d.price"), F.col("d.product_name"))
)

scored = (
    joined
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "last_known_quantity_2",
        "price",
        F.sum("last_known_quantity").over(w2).alias("running_last_known_quantity"),
        F.avg("last_known_quantity_2").over(w3).alias("last_known_quantity_2_avg_5"),
    )
)

with_windows = (
    scored
    .withColumn("running_max_last_known_quantity_2", F.max("last_known_quantity_2").over(w))
    .withColumn("running_sum_last_known_quantity_2", F.sum("last_known_quantity_2").over(w))
    .withColumn("running_count_last_known_quantity_2", F.count("last_known_quantity_2").over(w))
)

valid_inventory = (
    with_windows
    .filter((F.col("last_restock") >= F.lit("2023-06-23").cast("date")) & (F.col("last_known_quantity_2_avg_5") != 229))
    .withColumn("running_sum_last_known_quantity_2_int", F.col("running_sum_last_known_quantity_2").cast("int"))
    .withColumn("running_last_known_quantity_band", F.when(F.col("running_last_known_quantity") >= 1074, "High").when(F.col("running_last_known_quantity") >= 577, "Medium").otherwise("Low"))
)

result = (
    valid_inventory
    .groupBy(F.date_trunc("year", "last_restock").alias("year_start"))
    .agg(
        F.avg("last_known_quantity_2_avg_5").alias("avg_last_known_quantity_2_avg_5"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
)

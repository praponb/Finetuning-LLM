from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy(F.col("quantity_filled").desc(), "id")
w_part = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("product_id", "store_id").orderBy("days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("quantity").desc())
w6 = Window.partitionBy("product_id").orderBy(F.col("last_known_quantity_filled").desc_nulls_last(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2024-01-15").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(5).over(w_grp).alias("quantity_filled_bucket"),
        F.row_number().over(w_part).alias("row_seq"),
        F.min("quantity").over(w3).alias("product_id_min_quantity"),
        F.last("quantity_filled", ignorenulls=True).over(w4).alias("last_known_quantity_filled"),
        F.cume_dist().over(w5).alias("quantity_cume_dist"),
    )
)

filtered = (
    with_windows
    .filter((F.col("last_restock") > F.lit("2024-03-30").cast("date")) & (F.col("quantity_filled") >= 57))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_filled_bucket",
        "row_seq",
        "product_id_min_quantity",
        "last_known_quantity_filled",
        "quantity_cume_dist",
        F.round((F.col("quantity_filled_bucket") * 1.1), 2).alias("adjusted_quantity_filled_bucket"),
        F.ceil("product_id_min_quantity").alias("product_id_min_quantity_whole"),
    )
)

filtered_inventory = (
    filtered
    .filter(F.col("product_id_min_quantity").between(88, 137))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_filled_bucket",
        "row_seq",
        "last_known_quantity_filled",
        "quantity_cume_dist",
        (F.col("product_id_min_quantity") + F.col("quantity_cume_dist")).alias("product_id_min_quantity_plus_quantity_cume_dist"),
        (F.col("adjusted_quantity_filled_bucket") * F.col("row_seq")).alias("adjusted_quantity_filled_bucket_times_row_seq"),
    )
)

valid = (
    filtered_inventory
    .filter(F.col("quantity_filled_bucket") == 4)
    .select(
        "id",
        "product_id",
        "quantity_filled_bucket",
        "row_seq",
        "last_known_quantity_filled",
        F.when(F.col("adjusted_quantity_filled_bucket_times_row_seq") >= 65, "High").when(F.col("adjusted_quantity_filled_bucket_times_row_seq") >= 36, "Medium").otherwise("Low").alias("adjusted_quantity_filled_bucket_times_row_seq_tier"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "quantity_filled_bucket",
        "row_seq",
        "last_known_quantity_filled",
        "adjusted_quantity_filled_bucket_times_row_seq_tier",
        "latest_rank",
    )
    .orderBy(F.col("row_seq").desc())
)

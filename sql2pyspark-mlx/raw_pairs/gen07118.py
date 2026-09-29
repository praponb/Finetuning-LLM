from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_rounded").desc_nulls_last(), "id")
w2 = Window.orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy(F.col("quantity_rounded").desc(), "id")
w4 = Window.orderBy(F.col("price").desc_nulls_first())
w5 = Window.partitionBy(F.date_trunc("WEEK", "launch_date"))
w6 = Window.orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_restock", "id")
w8 = Window.orderBy("quantity_rounded", "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-07-26").cast("date")) & (F.col("quantity") != 145))
    .select(
        "*",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.when(F.col("quantity") >= 252, "High").when(F.col("quantity") >= 119, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .filter(F.size(F.col("d.tags")) >= 1)
    .select("s.*", F.col("d.launch_date"), F.col("d.price"), F.col("d.product_name"))
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_rounded",
        "launch_date",
        "price",
        "product_name",
    )
)

scored = (
    latest
    .withColumn("quantity_bucket", F.ntile(3).over(w2))
    .withColumn("quantity_rounded_bucket", F.ntile(5).over(w3))
    .withColumn("price_percent_rank", F.percent_rank().over(w4))
    .withColumn("quantity_bucket_2", F.ntile(3).over(w2))
    .withColumn("launch_date_period_stddev_samp_price", F.stddev_samp("price").over(w5))
)

scored_inventory = (
    scored
    .select(
        "id",
        "quantity",
        "last_restock",
        "launch_date",
        "price",
        "quantity_rounded_bucket",
        "quantity_bucket_2",
        "launch_date_period_stddev_samp_price",
        F.first("quantity_bucket_2", ignorenulls=True).over(w6).alias("first_known_quantity_bucket_2"),
        F.row_number().over(w7).alias("seq_no"),
        F.ntile(10).over(w8).alias("quantity_rounded_bucket_2"),
    )
)

result = (
    scored_inventory
    .select("id", "last_restock", "launch_date", "launch_date_period_stddev_samp_price")
)

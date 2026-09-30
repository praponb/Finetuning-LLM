from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "category").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("quantity_rounded").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand", "category").orderBy(F.col("greatest_quantity_rounded_bucket_quantity_rounded").desc(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("last_restock") > "2024-03-03")
    .withColumn("quantity_rounded", F.round("quantity", 1))
)

with_dims = (
    valid
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.brand"), F.col("d.category"))
)

with_windows = (
    with_dims
    .withColumn("cumulative_quantity", F.sum("quantity").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

metrics = (
    with_windows
    .select(
        "*",
        F.ntile(3).over(w3).alias("quantity_rounded_bucket"),
        F.stddev_samp("quantity").over(w4).alias("store_id_stddev_samp_quantity"),
        F.stddev_samp("quantity").over(w4).alias("store_id_stddev_samp_quantity_2"),
        F.last("cumulative_quantity", ignorenulls=True).over(w5).alias("last_known_cumulative_quantity"),
    )
)

valid_inventory = (
    metrics
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-09-15")))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity_rounded",
        "brand",
        "category",
        "cumulative_quantity",
        "row_seq",
        "quantity_rounded_bucket",
        "store_id_stddev_samp_quantity",
        "store_id_stddev_samp_quantity_2",
        "last_known_cumulative_quantity",
        F.greatest("quantity_rounded_bucket", "quantity_rounded").alias("greatest_quantity_rounded_bucket_quantity_rounded"),
    )
)

latest = (
    valid_inventory
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity_rounded",
        "brand",
        "category",
        "cumulative_quantity",
        "row_seq",
        "quantity_rounded_bucket",
        F.when(F.col("category").rlike("^(Grocery|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Home)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("category_zone"),
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("id").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "opened_date"), "product_id").orderBy(F.col("quantity_filled").desc(), "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy(F.year("opened_date"), "store_id").orderBy("quantity_filled", "id")
w6 = Window.partitionBy("product_id").orderBy("sqft_share", "id")
w7 = Window.partitionBy("store_id").orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "product_id").orderBy(F.col("opened_date").desc(), "id")
w10 = Window.partitionBy("product_id", "store_id")
w11 = Window.partitionBy("product_id").orderBy(F.col("quantity_filled_bucket").asc_nulls_first(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-09-27")))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity_filled",
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "store_id",
        "product_id",
        "opened_date",
        "sqft",
        F.ntile(3).over(w).alias("quantity_filled_bucket"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.last("sqft", ignorenulls=True).over(w3).alias("last_known_sqft"),
        F.ntile(10).over(w4).alias("sqft_bucket"),
        F.lag("quantity_filled", 2).over(w5).alias("prev_quantity_filled"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "product_id",
        "sqft",
        "quantity_filled_bucket",
        "sqft_bucket",
        "prev_quantity_filled",
        F.row_number().over(w6).alias("seq_no"),
        F.sum("sqft").over(w7).alias("running_sqft"),
        F.sum("sqft_share").over(w8).alias("running_sqft_share"),
        F.row_number().over(w_ordered).alias("seq_no_2"),
        F.round((F.col("sqft_bucket") / F.sum("sqft_bucket").over(w10)), 4).alias("sqft_bucket_share"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

remaining_keys = (
    latest
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 1)
        .select("product_id", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sqft",
        "quantity_filled_bucket",
        "sqft_bucket",
        "seq_no",
        "running_sqft",
        "sqft_bucket_share",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("seq_no").desc())
)

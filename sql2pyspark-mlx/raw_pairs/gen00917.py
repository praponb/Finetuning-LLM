from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity_whole").desc(), "id")
w3 = Window.orderBy("quantity_whole_bucket", "id").rowsBetween(-5, -1)
w4 = Window.orderBy(F.col("quantity").desc(), "id")
w5 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("week", "last_restock")).orderBy("quantity_bucket", "id")
w7 = Window.orderBy(F.col("quantity_bucket").desc_nulls_first())
w8 = Window.orderBy("quantity_whole_times_quantity_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("quantity_bucket_bucket", "id")
w10 = Window.orderBy("quantity_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("quantity_avg_5_prior", "id")
w12 = Window.orderBy(F.col("quantity_bucket").asc_nulls_last())

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-03-31").cast("date"))
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

with_windows = (
    valid
    .filter(F.col("quantity_whole").isNotNull())
    .withColumn("quantity_bucket", F.ntile(10).over(w))
    .withColumn("quantity_whole_bucket", F.ntile(3).over(w2))
)

valid_inventory = (
    with_windows
    .filter(F.col("last_restock") >= F.lit("2023-02-13").cast("date"))
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_bucket",
        "quantity_whole_bucket",
        (F.col("quantity_whole") * F.col("quantity_whole_bucket")).alias("quantity_whole_times_quantity_whole_bucket"),
        F.round((F.col("quantity") * 100), 2).alias("quantity_scaled"),
    )
)

enriched = (
    valid_inventory
    .select(
        "id",
        "quantity",
        "quantity_bucket",
        "quantity_whole_bucket",
        "quantity_whole_times_quantity_whole_bucket",
        "quantity_scaled",
        F.avg("quantity").over(w3).alias("quantity_avg_5_prior"),
        F.row_number().over(w4).alias("seq_no"),
        F.first("quantity_bucket", ignorenulls=True).over(w5).alias("first_known_quantity_bucket"),
        F.ntile(3).over(w6).alias("quantity_bucket_bucket"),
        F.cume_dist().over(w7).alias("quantity_bucket_cume_dist"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "quantity",
        "quantity_whole_bucket",
        "quantity_whole_times_quantity_whole_bucket",
        "quantity_avg_5_prior",
        "first_known_quantity_bucket",
        F.first("first_known_quantity_bucket", ignorenulls=True).over(w8).alias("first_known_first_known_quantity_bucket"),
        F.ntile(10).over(w_part).alias("quantity_bucket_bucket_bucket"),
        F.sum("quantity_bucket").over(w10).alias("cumulative_quantity_bucket"),
        F.ntile(5).over(w_ordered).alias("quantity_avg_5_prior_bucket"),
        F.rank().over(w12).alias("quantity_bucket_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "quantity",
        "quantity_whole_times_quantity_whole_bucket",
        "first_known_quantity_bucket",
        "first_known_first_known_quantity_bucket",
        "quantity_bucket_bucket_bucket",
        "quantity_avg_5_prior_bucket",
    )
)

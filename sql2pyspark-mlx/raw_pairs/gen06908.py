from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category", "brand").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("category").orderBy("launch_date_period", "id")
w5 = Window.partitionBy("category").orderBy("launch_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("seq_no_tier").orderBy(F.col("burst_id_per_home").desc())

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Initech")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        F.date_trunc("WEEK", "launch_date").alias("launch_date_period"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "launch_date_period",
        F.stddev_samp("cost").over(w).alias("brand_stddev_samp_cost"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

gaps = (
    with_latest
    .withColumn("new_group_flag", F.when(F.lag("launch_date_period").over(w4).isNull() | (F.col("launch_date_period") > F.lag("launch_date_period").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "category",
        "stock_qty",
        "launch_date_period",
        "seq_no",
        "last_restock",
        F.sum("new_group_flag").over(w5).alias("burst_id"),
    )
)

filtered = (
    sessionized
    .filter(F.col("burst_id") > 3)
    .select(
        "id",
        "category",
        "stock_qty",
        "seq_no",
        "last_restock",
        "burst_id",
        F.when(F.col("stock_qty") >= 389, "High").when(F.col("stock_qty") >= 193, "Medium").otherwise("Low").alias("stock_qty_band"),
        F.upper("category").alias("category_upper"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        F.date_trunc("month", "last_restock").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Grocery", F.col("seq_no")).otherwise(0)).alias("grocery_seq_no"),
        F.sum(F.when(F.col("category") == "Books", F.col("burst_id"))).alias("books_total_burst_id"),
        F.max("seq_no").alias("max_seq_no"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("burst_id_per_home"),
    )
    .withColumn("burst_id_per_home_rank", F.rank().over(w6))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("region").orderBy("sqft_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("order_order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("sqft_rounded").desc(), "id")
w5 = Window.partitionBy(F.month("order_order_date")).orderBy("order_order_date", "id")
w6 = Window.partitionBy("city").orderBy("prev_sqft_rounded", "id")
w7 = Window.orderBy(F.col("running_sqft_rounded_2").desc(), "id")
w8 = Window.partitionBy("city").orderBy(F.col("running_sqft_rounded").desc(), "id")
w9 = Window.partitionBy("city")
w10 = Window.orderBy("running_sqft_rounded_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("Central"))) & (F.col("opened_date") >= F.lit("2023-08-07").cast("date")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.round("sqft", 0).alias("sqft_rounded"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.sqft_rounded",
        "s.store_name_normalized",
        F.col("c.order_date").alias("order_order_date"),
    )
)

metrics = (
    latest_match
    .select(
        "id",
        "city",
        "region",
        "store_name_normalized",
        F.sum("sqft_rounded").over(w_hist).alias("running_sqft_rounded"),
        F.sum("sqft_rounded").over(w3).alias("running_sqft_rounded_2"),
        F.ntile(4).over(w4).alias("sqft_rounded_bucket"),
        F.lag("sqft_rounded", 2).over(w5).alias("prev_sqft_rounded"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel") != "mobile"), "left_semi")
)

with_windows = (
    kept
    .filter(F.col("sqft_rounded_bucket") >= 5)
    .select(
        "*",
        F.ntile(10).over(w6).alias("prev_sqft_rounded_bucket"),
        F.ntile(10).over(w7).alias("running_sqft_rounded_2_bucket"),
        F.ntile(4).over(w8).alias("running_sqft_rounded_bucket"),
        F.round((F.col("sqft_rounded_bucket") / F.sum("sqft_rounded_bucket").over(w9)), 4).alias("sqft_rounded_bucket_share"),
        F.sum("running_sqft_rounded").over(w10).alias("running_running_sqft_rounded"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "store_name_normalized",
        "running_sqft_rounded",
        "running_sqft_rounded_2",
        "sqft_rounded_bucket",
        "prev_sqft_rounded",
        "running_sqft_rounded_2_bucket",
        "running_sqft_rounded_bucket",
        F.when(F.col("city").rlike("^(Tokyo|London)$"), "PRIORITY").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy("city")
)

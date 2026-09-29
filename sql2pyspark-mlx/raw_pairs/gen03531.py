from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue").over(w_running).alias("next_revenue"),
        F.lag("units").over(w_running).alias("prev_units"),
        F.lead("units").over(w_running).alias("next_units"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "sale_date", "units", "next_revenue", "prev_units")
)

valid = (
    deduped
    .filter((F.col("sale_date") >= F.lit("2023-03-09").cast("date")) & (F.col("units") > 38))
    .select(
        "*",
        F.when(F.col("units") >= 39, "High").when(F.col("units") >= 13, "Medium").otherwise("Low").alias("units_band"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.sqft"), F.col("d.opened_date"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .filter(F.year(F.col("d.opened_date")) == 2023)
    .select(
        "e.*",
        F.col("d.store_name"),
        F.col("d.opened_date").alias("store_opened_date"),
        F.col("d.city"),
    )
)

pivoted = (
    joined
    .groupBy("store_id")
    .pivot("city", ["London", "Berlin", "Bangkok"])
    .agg(F.sum("next_revenue").alias("amount"))
)

result = (
    pivoted
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select("store_id", "London", "Berlin", "Bangkok", F.col("min_order_shipping_fee"))
    .orderBy(F.col("London").asc_nulls_first())
)

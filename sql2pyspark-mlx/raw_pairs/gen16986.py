from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w_grp = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city", "order_status").orderBy("region_max_is_tokyo", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("region_max_is_tokyo", "id")
w6 = Window.partitionBy("order_status", "city").orderBy(F.col("prev_first_known_sqft").desc(), "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("sqft") <= 7327))
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.when(F.col("city") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

with_windows = (
    valid
    .filter(F.col("is_tokyo") >= 61)
    .withColumn("region_max_is_tokyo", F.max("is_tokyo").over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w_grp))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.sqft",
        "s.is_tokyo",
        "s.region_max_is_tokyo",
        "s.first_known_sqft",
        F.col("c.status").alias("order_status"),
        F.col("c.discount"),
    )
)

filtered = (
    latest_match
    .filter(F.col("first_known_sqft") <= 4727)
    .withColumn("sqft_times_region_max_is_tokyo", (F.col("sqft") * F.col("region_max_is_tokyo")))
)

enriched = (
    filtered
    .select(
        "id",
        "city",
        "sqft",
        "order_status",
        F.avg("sqft_times_region_max_is_tokyo").over(w4).alias("sqft_times_region_max_is_tokyo_avg_5"),
        F.lag("first_known_sqft", 2, 0).over(w5).alias("prev_first_known_sqft"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

kept = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.customer_id").isin([1, 2])), "left_semi")
)

result = (
    kept
    .groupBy("city")
    .pivot("order_status", ["RETURNED", "COMPLETED", "SHIPPED"])
    .agg(F.sum("sqft"))
    .select(
        "city",
        F.col("RETURNED").alias("returned"),
        F.col("COMPLETED").alias("completed"),
        F.col("SHIPPED").alias("shipped"),
    )
)

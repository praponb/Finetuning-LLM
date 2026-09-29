from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("opened_date"), "region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.orderBy(F.col("sqft").desc(), "id")
w4 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("WEEK", "opened_date")).orderBy(F.col("sqft").desc(), "id")
w6 = Window.partitionBy("region").orderBy("sqft_bucket", "id")
w7 = Window.partitionBy("city", "region").orderBy("sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-07-27").cast("date")) & (F.col("opened_date") >= "2023-05-21"))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
)

with_windows = (
    filtered
    .withColumn("running_sqft", F.sum("sqft").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("sqft_bucket", F.ntile(10).over(w3))
    .withColumn("running_sqft_2", F.sum("sqft").over(w4))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w5))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 40), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "running_sqft",
        "sqft_bucket",
        "running_sqft_2",
        F.row_number().over(w6).alias("seq_no"),
        F.first("sqft_bucket", ignorenulls=True).over(w7).alias("first_known_sqft_bucket"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "running_sqft",
        "sqft_bucket",
        "first_known_sqft_bucket",
    )
)

result = (
    ranked
    .withColumn("quarter_start", F.date_trunc("quarter", "opened_date"))
    .withColumn("sqft_bucket_tier", F.when(F.col("sqft_bucket") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "sqft_bucket_tier"], ["quarter_start"], []],
        "quarter_start",
        "sqft_bucket_tier",
    )
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.avg(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_avg_sqft"),
    )
    .orderBy("south_avg_sqft")
)

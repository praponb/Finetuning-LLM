from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.year("opened_date"), "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.orderBy(F.col("sqft").desc())
w_hist = Window.orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter(F.col("region").eqNullSafe(F.lit("West")))
    .withColumn("store_name_normalized", F.upper(F.trim("store_name")))
    .withColumn("region_filled", F.coalesce("region", F.lit("unassigned")))
)

metrics = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w2))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
    .withColumn("seq_no_2", F.row_number().over(w4))
)

cleaned = (
    metrics
    .filter((F.col("opened_date") > F.lit("2023-03-20").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "store_name_normalized",
        "prev_sqft",
        "cumulative_sqft",
        (F.col("seq_no") + F.col("prev_sqft")).alias("seq_no_plus_prev_sqft"),
    )
)

metrics_stores = (
    cleaned
    .filter(F.col("opened_date") <= "2024-07-01")
    .select(
        "*",
        F.rank().over(w5).alias("sqft_rank"),
        F.last("cumulative_sqft", ignorenulls=True).over(w_hist).alias("last_known_cumulative_sqft"),
    )
)

filtered = (
    metrics_stores
    .filter((F.col("opened_date") >= F.lit("2023-06-05").cast("date")) & (F.col("opened_date") < "2025-06-01"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

result = (
    filtered
    .select(
        "id",
        "store_name",
        "sqft",
        "store_name_normalized",
        "sqft_rank",
        "last_known_cumulative_sqft",
        "days_since_opened_date",
    )
    .orderBy(F.col("sqft_rank").asc_nulls_first())
)

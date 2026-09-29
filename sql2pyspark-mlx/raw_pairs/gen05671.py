from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region")
w_ordered = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("region_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "region").orderBy("region_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_sum_region_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.month("opened_date")).orderBy("running_max_region_sum_sqft", "id")
w8 = Window.orderBy(F.col("running_count_region_sum_sqft").desc_nulls_last())
w9 = Window.orderBy("running_count_region_sum_sqft", "id").rowsBetween(-2, Window.currentRow)
w10 = Window.orderBy("running_sqft", "id").rowsBetween(-3, -1)
w11 = Window.orderBy("running_avg_region_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy(F.month("opened_date")).orderBy("running_avg_region_sum_sqft", "id")
w13 = Window.partitionBy(F.date_trunc("WEEK", "opened_date")).orderBy("running_max_region_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select("*", F.lower("city").alias("city_lower"))
)

with_windows = (
    filtered
    .filter(F.col("opened_date") > "2025-10-01")
    .select(
        "*",
        F.sum("sqft").over(w_grp).alias("region_sum_sqft"),
        F.first("region", ignorenulls=True).over(w_ordered).alias("first_known_region"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "city_lower",
        "first_known_region",
        F.max("region_sum_sqft").over(w3).alias("running_max_region_sum_sqft"),
        F.avg("region_sum_sqft").over(w3).alias("running_avg_region_sum_sqft"),
        F.sum("region_sum_sqft").over(w3).alias("running_sum_region_sum_sqft"),
        F.count("region_sum_sqft").over(w3).alias("running_count_region_sum_sqft"),
        F.sum("sqft").over(w4).alias("running_sqft"),
    )
)

with_windows_stores = (
    scored
    .select(
        "*",
        F.first("running_sum_region_sum_sqft", ignorenulls=True).over(w5).alias("first_known_running_sum_region_sum_sqft"),
        F.first("running_sqft", ignorenulls=True).over(w6).alias("first_known_running_sqft"),
        F.lead("sqft", 2).over(w7).alias("next_sqft"),
        F.rank().over(w8).alias("running_count_region_sum_sqft_rank"),
    )
)

enriched = (
    with_windows_stores
    .withColumn("running_max_region_sum_sqft_avg_2", F.avg("running_max_region_sum_sqft").over(w9))
    .withColumn("running_max_region_sum_sqft_avg_3_prior", F.avg("running_max_region_sum_sqft").over(w10))
    .withColumn("first_known_running_count_region_sum_sqft", F.first("running_count_region_sum_sqft", ignorenulls=True).over(w11))
    .withColumn("running_avg_region_sum_sqft_bucket", F.ntile(10).over(w12))
    .withColumn("last_known_running_count_region_sum_sqft", F.last("running_count_region_sum_sqft", ignorenulls=True).over(w13))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "first_known_region",
        "running_max_region_sum_sqft",
        "running_avg_region_sum_sqft",
        "running_count_region_sum_sqft",
        "first_known_running_sqft",
    )
    .orderBy(F.col("id").desc())
)

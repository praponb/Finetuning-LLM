from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date")).orderBy("revenue")
w2 = Window.orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("week", "sale_date"), "store_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("revenue_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_last(), "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-07-18"))) & F.col("sale_date").between("2023-06-01", "2024-06-01"))
    .select("*", F.coalesce("revenue", F.lit(0)).alias("revenue_filled"))
)

enriched = (
    base
    .filter(F.col("store_id") == 3)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.rank().over(w).alias("revenue_rank"),
        F.avg("revenue_filled").over(w2).alias("revenue_filled_avg_3"),
        F.ntile(3).over(w3).alias("units_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("units_bucket").over(w4).alias("units_bucket_avg_2"),
        F.sum("units").over(w5).alias("cumulative_units"),
        F.last("units", ignorenulls=True).over(w6).alias("last_known_units"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue_rank",
        "revenue_filled_avg_3",
        "units_bucket_avg_2",
        "cumulative_units",
    )
)

cleaned = (
    latest
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-04-22"))) & F.col("store_id").isin([1, 4, 5, 6]))
    .select(
        "*",
        (F.col("revenue_filled_avg_3") * F.col("units_bucket_avg_2")).alias("revenue_filled_avg_3_times_units_bucket_avg_2"),
        F.greatest("revenue_filled_avg_3", "revenue_rank").alias("greatest_revenue_filled_avg_3_revenue_rank"),
    )
)

base_store_sales = (
    cleaned
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-30")))
    .select(
        "*",
        F.ceil("greatest_revenue_filled_avg_3_revenue_rank").alias("greatest_revenue_filled_avg_3_revenue_rank_whole"),
        (F.col("revenue_filled_avg_3_times_units_bucket_avg_2") + F.col("greatest_revenue_filled_avg_3_revenue_rank")).alias("revenue_filled_avg_3_times_units_bucket_avg_2_plus_greatest_revenue_filled_avg_3_revenue_rank"),
    )
)

result = (
    base_store_sales
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.region").alias("region"), F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.units_bucket_avg_2")).alias("min_units_bucket_avg_2"),
        F.sum(F.col("a.cumulative_units")).alias("total_cumulative_units"),
        F.grouping_id().alias("gid"),
    )
)

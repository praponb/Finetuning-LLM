from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("store_id").orderBy("island_id", "id")
w_part = Window.partitionBy(F.month("sale_date_period"))

base = (
    df_store_sales
    .filter(F.col("revenue") <= 4000)
    .select(
        "*",
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
    )
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("sale_date_period").over(w).isNull() | (F.col("sale_date_period") > F.lag("sale_date_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "sale_date_period",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

enriched = (
    islands
    .filter(F.col("units") >= 20)
    .select(
        "id",
        "store_id",
        "units",
        "island_id",
        F.avg("units").over(w3).alias("units_avg_2_prior"),
        F.lead("island_id", 1).over(w4).alias("next_island_id"),
        F.stddev_samp("island_id").over(w_part).alias("sale_date_period_month_stddev_samp_island_id"),
    )
)

enriched_store_sales = (
    enriched
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.units",
        "e.island_id",
        "e.units_avg_2_prior",
        "e.next_island_id",
        "e.sale_date_period_month_stddev_samp_island_id",
        F.col("d.city"),
        F.col("d.opened_date"),
        F.col("d.region"),
    )
)

result = (
    enriched_store_sales
    .groupBy("island_id", "region")
    .agg(
        F.percentile_approx("sale_date_period_month_stddev_samp_island_id", 0.9).alias("sale_date_period_month_stddev_samp_island_id_p90"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.countDistinct("region").alias("distinct_regions"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .orderBy(F.col("east_count").desc())
)

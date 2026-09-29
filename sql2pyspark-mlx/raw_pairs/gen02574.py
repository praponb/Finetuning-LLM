from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("session_id", "id").rowsBetween(-6, -1)
w4 = Window.orderBy(F.col("sale_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-21")))
    .withColumn("sale_date_period", F.date_trunc("YEAR", "sale_date"))
)

joined = (
    filtered
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.units",
        "src.sale_date_period",
        F.col("b.store_name"),
    )
)

flagged = (
    joined
    .withColumn("is_new_session", F.when(F.lag("sale_date_period").over(w).isNull() | (F.col("sale_date_period") > F.lag("sale_date_period").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "sale_date_period",
        "store_name",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

with_windows = (
    islands
    .withColumn("units_avg_6_prior", F.avg("units").over(w3))
    .withColumn("units_sum_30d", F.sum("units").over(w4))
)

filtered_store_sales = (
    with_windows
    .filter(F.col("sale_date") > F.lit("2023-07-17").cast("date"))
    .select(
        "id",
        "sale_date",
        "units",
        "store_name",
        "session_id",
        "units_avg_6_prior",
        "units_sum_30d",
        F.to_date("sale_date_period").alias("sale_date_period_date"),
    )
)

result = (
    filtered_store_sales
    .withColumn("units_avg_6_prior_tier", F.when(F.col("units_avg_6_prior") >= 35, "HIGH").otherwise("LOW"))
    .rollup("session_id", "units_avg_6_prior_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("units").alias("units_bit_and"),
        F.max("units_avg_6_prior").alias("max_units_avg_6_prior"),
        F.count("*").alias("cnt"),
        F.grouping("session_id").alias("grouping_id"),
    )
    .orderBy(F.col("max_units_avg_6_prior").asc_nulls_last())
)

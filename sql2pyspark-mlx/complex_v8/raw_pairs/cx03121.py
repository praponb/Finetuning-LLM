from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy("premium", "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")

valid = (
    df_policies
    .filter((F.col("product").isNotNull()) | (F.col("start_date") >= F.lit("2024-06-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.date_trunc("year", F.col("end_date")).alias("end_date_year"),
    )
)

unique_rows = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.count(F.lit(1)).over(w_holder_id_product).alias("rows_in_holder_id_product"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_product), 2).alias("premium_vs_holder_id_product_avg"),
    )
)

summary = (
    with_trends
    .groupBy(F.date_trunc("month", "end_date_year").alias("end_date_year_month"))
    .agg(
        F.sum(F.when(F.col("product").isNotNull(), F.col("premium"))).alias("premium_product_is_not_null"),
        F.round(F.stddev("premium"), 2).alias("stddev_premium"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("product") == "TRAVEL", F.col("premium")).otherwise(0)).alias("premium_product_travel"),
        F.count(F.when(F.col("product").isin("TRAVEL", "HOME"), 1)).alias("n_product_in_travel_home"),
    )
)

result = (
    summary
    .filter(F.col("stddev_premium") > 1250.0)
    .select(
        "end_date_year_month",
        "premium_product_is_not_null",
        "premium_product_travel",
        (F.col("end_date_year_month") + F.expr("INTERVAL 15 MINUTES")).alias("end_date_year_month_plus_15m"),
        F.date_format(F.col("end_date_year_month"), "yyyy-MM").alias("end_date_year_month_label"),
    )
)

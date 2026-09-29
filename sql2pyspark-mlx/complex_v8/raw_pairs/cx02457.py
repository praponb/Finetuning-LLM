from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("product").orderBy(F.col("start_date").desc(), F.col("id").desc())
w_status_start_date_month = Window.partitionBy("status", F.month("start_date"))
w_all_by_premium = Window.orderBy("premium", "id")

recent_policys = (
    df_policies
    .filter((F.col("status") == "LAPSED") | (F.col("product") != "HOME"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

unique_rows = (
    recent_policys
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    unique_rows
    .select(
        "*",
        F.sum("premium_filled").over(w_status_start_date_month).alias("status_start_date_month_sum_premium_filled"),
        (F.col("premium_filled") - F.lag("premium_filled").over(w_all_by_premium)).alias("premium_filled_change"),
        F.max("premium_filled").over(w_status_start_date_month).alias("status_start_date_month_max_premium_filled"),
        F.round(F.avg("premium_filled").over(w_status_start_date_month), 2).alias("status_start_date_month_avg_premium_filled"),
    )
)

result = (
    ranked
    .groupBy("holder_id")
    .pivot("product", ["AUTO", "HOME", "HEALTH", "TRAVEL"])
    .agg(F.count("premium").alias("count_v"), F.sum("premium").alias("sum_v"))
)

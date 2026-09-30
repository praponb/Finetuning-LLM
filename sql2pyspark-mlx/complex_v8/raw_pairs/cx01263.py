from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_start_date_month_by_premium = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("premium_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_policies
    .filter(F.col("product") != "AUTO")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        F.nth_value("end_date", 2).over(w_start_date_month_by_premium).alias("second_end_date"),
    )
)

result = (
    metrics
    .groupBy("holder_id", "start_date")
    .pivot("product", ["HEALTH", "HOME"])
    .agg(F.sum("premium").alias("total"), F.max("premium").alias("max_v"))
    .orderBy("holder_id", "start_date")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status").orderBy("days_start_date_to_end_date", "id")
w_product_status_by_end_date = Window.partitionBy("product", "status").orderBy("end_date", "id")
w_start_date_month_status = Window.partitionBy(F.date_trunc("month", "start_date"), "status")

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") <= 19) & (F.col("premium").between(918.1, 3817.5)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

unique_rows = (
    filtered_policys
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    unique_rows
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "status_prefix",
        F.row_number().over(w_product_status_by_end_date).alias("rn"),
        F.max("days_start_date_to_end_date").over(w_start_date_month_status).alias("start_date_month_status_max_days_start_date_to_e"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        "status_prefix",
        "rn",
        "start_date_month_status_max_days_start_date_to_e",
        F.concat(F.col("status"), F.lit("#"), F.col("rn").cast("string")).alias("status_ref"),
        F.abs(F.col("rn") - 5).alias("rn_distance"),
    )
    .orderBy("start_date")
)

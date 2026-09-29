from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product_by_premium_bu = Window.partitionBy("status", "product").orderBy("premium_bucket", "id")
w_holder_id_status = Window.partitionBy("holder_id", "status")

prepared_policys = (
    df_policies
    .filter(F.col("premium") > 2289.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

with_windows = (
    prepared_policys
    .select(
        "*",
        F.ntile(5).over(w_status_product_by_premium_bu).alias("premium_bucket_quintile"),
        F.sum("premium_bucket").over(w_holder_id_status).alias("holder_id_status_sum_premium_bucket"),
        F.round(F.col("days_since_start_date") - F.avg("days_since_start_date").over(w_holder_id_status), 2).alias("days_since_start_date_vs_holder_id_status_avg"),
    )
)

result = (
    with_windows
    .select("id", "days_since_start_date_vs_holder_id_status_avg", "premium")
    .unpivot(
        ids=["id"],
        values=["days_since_start_date_vs_holder_id_status_avg", "premium"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

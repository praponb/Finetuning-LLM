from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status_by_days_start = Window.partitionBy("product", "status").orderBy("days_start_date_to_end_date", "id")
w_product_status_by_days_start_2 = Window.partitionBy("product", "status").orderBy("days_start_date_to_end_date")
w_holder_id_status = Window.partitionBy("holder_id", "status")

valid = (
    df_policies
    .filter((~F.col("product").isin("HOME", "TRAVEL")) & (F.col("status").isin("CANCELLED", "EXPIRED")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.lead("days_start_date_to_end_date", 3).over(w_product_status_by_days_start).alias("next3_days_start_date_to_end_date"),
        F.round(F.cume_dist().over(w_product_status_by_days_start_2), 3).alias("days_start_date_to_end_date_cume_dist"),
        F.round(F.avg("days_start_date_to_end_date").over(w_holder_id_status), 2).alias("holder_id_status_avg_days_start_date_to_end_date"),
    )
)

summary = (
    with_trends
    .groupBy("product", "status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.min("start_date").alias("first_start_date"),
        F.count("*").alias("cnt"),
    )
)

filtered = (
    summary
    .crossJoin(summary.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

result = (
    filtered
    .select(
        "product",
        "status",
        F.col("distinct_status").alias("distinct_status_v"),
        F.col("cnt").alias("cnt_v"),
    )
    .unpivot(
        ids=["product", "status"],
        values=["distinct_status_v", "cnt_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

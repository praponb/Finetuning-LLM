from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date = Window.orderBy(F.col("start_date").desc_nulls_first(), "id")
w_holder_id_product = Window.partitionBy("holder_id", "product")
w_holder_id_end_date_month_by = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("start_date_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_end_date_month_by_2 = Window.partitionBy("holder_id", F.date_trunc("month", "end_date")).orderBy(F.col("start_date_month").desc())

valid_policys = (
    df_policies
    .filter((F.col("status").isin("CANCELLED", "LAPSED", "EXPIRED")) & (F.col("start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-05-02").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

with_windows = (
    valid_policys
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "premium_distance",
        "start_date_month",
        F.dense_rank().over(w_all_by_start_date).alias("start_date_dense_rank"),
        F.size(F.collect_set("status").over(w_holder_id_product)).alias("distinct_status_in_holder_id_product"),
        F.nth_value("premium_filled", 2).over(w_holder_id_end_date_month_by).alias("second_premium_filled"),
        F.dense_rank().over(w_holder_id_end_date_month_by_2).alias("start_date_month_dense_rank"),
    )
)

scored = (
    with_windows
    .filter(F.col("start_date_dense_rank") <= 6)
    .withColumn("start_date_month_end", F.last_day(F.col("start_date")))
)

result = (
    scored
    .select("id", "start_date_dense_rank", "start_date_month_dense_rank")
    .unpivot(
        ids=["id"],
        values=["start_date_dense_rank", "start_date_month_dense_rank"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

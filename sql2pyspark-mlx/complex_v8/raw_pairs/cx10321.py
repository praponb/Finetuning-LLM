from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_holder_id_by_days_since_end = Window.partitionBy("holder_id").orderBy(F.col("days_since_end_date").desc(), "id")

cleaned = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2027-03-20").cast("date"))))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "days_since_end_date",
        F.round(F.col("premium_bucket") / F.nullif(F.sum("premium_bucket").over(w_status), F.lit(0)), 4).alias("premium_bucket_share_of_status"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_holder_id_by_days_since_end)).alias("days_since_prev_end_date"),
        F.lead("days_since_end_date", 3).over(w_holder_id_by_days_since_end).alias("next3_days_since_end_date"),
        F.lag("end_date", 3).over(w_holder_id_by_days_since_end).alias("prev3_end_date"),
        F.rank().over(w_holder_id_by_days_since_end).alias("days_since_end_date_rank"),
        F.dense_rank().over(w_holder_id_by_days_since_end).alias("days_since_end_date_dense_rank"),
    )
)

holder_id_stats = (
    with_windows
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

compared = (
    with_windows.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        "premium_bucket_share_of_status",
        "premium_minus_max",
        "premium",
    )
    .unpivot(
        ids=["id"],
        values=["premium_bucket_share_of_status", "premium_minus_max", "premium"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

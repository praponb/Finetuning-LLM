from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_quality_len_ties = Window.orderBy("quality_len")
w_all_by_quality_len_full = Window.orderBy("quality_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_quality_len = Window.orderBy("quality_len", "id")

base_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

windowed = (
    base_readings
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_quality_len_ties), 3).alias("quality_len_percent_rank"),
        F.nth_value("kwh", 2).over(w_all_by_quality_len_full).alias("second_kwh"),
        F.rank().over(w_all_by_quality_len).alias("quality_len_rank"),
        F.dense_rank().over(w_all_by_quality_len).alias("quality_len_dense_rank"),
    )
)

stats = (
    windowed
    .groupBy("quality_len_rank", "quality", "quality_len")
    .agg(
        F.sum("quality_len").alias("total_quality_len"),
        F.min("read_at").alias("first_read_at"),
        F.count("*").alias("cnt"),
        F.max("quality_len_dense_rank").alias("max_quality_len_dense_rank_check"),
    )
    .filter(F.col("max_quality_len_dense_rank_check") > 3)
    .drop("max_quality_len_dense_rank_check")
)

result = (
    stats
    .select(
        "quality_len_rank",
        "quality",
        "quality_len",
        "total_quality_len",
    )
    .unpivot(
        ids=["quality_len_rank", "quality"],
        values=["quality_len", "total_quality_len"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("quality_len_rank", "quality", "metric")
)

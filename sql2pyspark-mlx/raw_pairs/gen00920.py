from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy(F.year("signup_date"), "city").orderBy(F.col("credit_limit").desc_nulls_first())
w3 = Window.partitionBy("country").orderBy("credit_limit_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment", "city")

base = (
    df_customers
    .filter(F.col("age") != 37)
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

with_windows = (
    base
    .filter(F.col("country").isin(["US", "JP"]))
    .select(
        "*",
        F.last("segment", ignorenulls=True).over(w).alias("last_known_segment"),
        F.dense_rank().over(w_recent).alias("credit_limit_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "name",
        "segment",
        "is_active",
        "segment_clean",
        "last_known_segment",
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
        F.sum("credit_limit_dense_rank").over(w4).alias("segment_sum_credit_limit_dense_rank"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.country").isin(["DE", "US", "TH"])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "segment",
        "is_active",
        "segment_clean",
        "last_known_segment",
        "segment_sum_credit_limit_dense_rank",
    )
    .orderBy("segment")
)

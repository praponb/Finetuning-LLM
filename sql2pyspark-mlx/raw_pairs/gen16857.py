from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.orderBy("months_since_started_at")
w3 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy(F.col("months_since_started_at").asc_nulls_first())
w5 = Window.partitionBy("browser").orderBy(F.col("months_since_started_at").asc_nulls_last())

valid = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox"]))
    .select(
        "*",
        (F.datediff(F.current_date(), "started_at") / 30).alias("months_since_started_at"),
        F.upper("country").alias("country_upper"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "browser",
        "started_at",
        "months_since_started_at",
        "browser_clean",
        F.sum("months_since_started_at").over(w).alias("months_since_started_at_sum_7d"),
        F.rank().over(w_recent).alias("months_since_started_at_rank"),
        F.avg("months_since_started_at").over(w3).alias("months_since_started_at_avg_30d"),
        F.cume_dist().over(w_hist).alias("months_since_started_at_cume_dist"),
        F.dense_rank().over(w5).alias("months_since_started_at_dense_rank"),
    )
)

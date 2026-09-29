from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id")
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").asc_nulls_last())
w_dedup = Window.partitionBy("team").orderBy("rn", "id")

prepared_agents = (
    df_support_agents
    .filter((F.col("team").isNotNull()) & (F.col("tier").isin("T3", "T1")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

metrics = (
    prepared_agents
    .select(
        "id",
        "team",
        "tier",
        F.round(F.percent_rank().over(w_all_by_hired_on), 3).alias("hired_on_percent_rank"),
        F.dense_rank().over(w_all_by_hired_on_ties).alias("hired_on_dense_rank"),
        F.row_number().over(w_all_by_hired_on).alias("rn"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
    )
)

unique_rows = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

top_per_group = unique_rows.filter(F.col("hired_on_dense_rank") == 1)

result = (
    top_per_group
    .select(
        "id",
        "team",
        "tier",
        "hired_on_percent_rank",
        "hired_on_dense_rank",
        "rn",
        "days_since_prev_hired_on",
        (F.col("days_since_prev_hired_on") - F.col("hired_on_dense_rank")).alias("days_since_prev_hired_on_minus_hired_on_dense_ra"),
    )
    .orderBy(F.col("team").desc(), "hired_on_dense_rank", "id")
    .limit(10)
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_1d_opened_at_mont = Window.partitionBy("prior_rows_1d", F.month("opened_at"))

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.requester_id").alias("prior_requester_id_1d"),
    )
)

with_trends = (
    joined_ranges
    .withColumn("rows_in_prior_rows_1d_opened_at_month", F.count(F.lit(1)).over(w_prior_rows_1d_opened_at_mont))
    .withColumn("prior_rows_1d_opened_at_month_sum_prior_requeste", F.sum("prior_requester_id_1d").over(w_prior_rows_1d_opened_at_mont))
)

result = (
    with_trends
    .select(
        "id",
        "prior_rows_1d",
        "prior_requester_id_1d",
        "rows_in_prior_rows_1d_opened_at_month",
        F.coalesce(F.col("prior_rows_1d"), F.lit(0)).alias("prior_rows_1d_filled"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)

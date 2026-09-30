from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_channels_opened_at = Window.partitionBy("distinct_channels", F.year("opened_at_day")).orderBy("n_rows", "opened_at_day", "closed_at_day")
w_opened_at_day_month_distinct = Window.partitionBy(F.month("opened_at_day"), "distinct_channels")

base = (
    df_tickets
    .filter(F.col("csat") <= 4)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
        F.when(F.col("requester_id") >= 19, "large").when(F.col("requester_id") >= 6, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

per_group = (
    base
    .filter(F.col("csat") > 2)
    .groupBy(
        F.to_date("opened_at").alias("opened_at_day"),
        F.to_date("closed_at").alias("closed_at_day"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.max("hours_opened_at_to_closed_at").alias("max_hours_opened_at_to_closed_at"),
        F.sum(F.when(F.col("priority").isNotNull(), 1).otherwise(0)).alias("n_priority_is_not_null"),
        F.round(F.stddev("csat"), 2).alias("stddev_csat"),
        F.sum("csat").alias("total_csat"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .filter(F.col("n_rows") >= 2)
)

metrics = (
    per_group
    .select(
        "opened_at_day",
        "closed_at_day",
        "n_rows",
        "max_hours_opened_at_to_closed_at",
        "n_priority_is_not_null",
        "total_csat",
        F.ntile(4).over(w_distinct_channels_opened_at).alias("n_rows_quartile"),
        F.count(F.lit(1)).over(w_opened_at_day_month_distinct).alias("rows_in_opened_at_day_month_distinct_channels"),
    )
)

tagged = (
    metrics
    .select(
        "opened_at_day",
        "closed_at_day",
        "n_rows",
        "max_hours_opened_at_to_closed_at",
        "n_priority_is_not_null",
        "total_csat",
        "n_rows_quartile",
        "rows_in_opened_at_day_month_distinct_channels",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at_day")).alias("days_since_opened_at_day"),
    )
)

result = (
    tagged
    .select(
        "opened_at_day",
        "closed_at_day",
        "n_rows_quartile",
        "n_rows",
        "days_since_opened_at_day",
    )
    .unpivot(
        ids=["opened_at_day", "closed_at_day"],
        values=["n_rows_quartile", "n_rows", "days_since_opened_at_day"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("opened_at_day", "closed_at_day", "metric")
)

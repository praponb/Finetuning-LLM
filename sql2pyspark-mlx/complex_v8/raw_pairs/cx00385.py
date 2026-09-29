from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_csat_12h_runnin = Window.orderBy(F.col("prior_csat_12h").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_prior_rows_12h = Window.partitionBy("agent_id", "prior_rows_12h")
w_all_by_prior_csat_12h = Window.orderBy(F.col("prior_csat_12h").asc_nulls_last(), "id")

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

with_trends = (
    covered
    .select(
        "*",
        F.first("prior_rows_12h", ignorenulls=True).over(w_all_by_prior_csat_12h_runnin).alias("first_known_prior_rows_12h"),
        F.min("prior_csat_12h").over(w_agent_id_prior_rows_12h).alias("agent_id_prior_rows_12h_min_prior_csat_12h"),
        F.row_number().over(w_all_by_prior_csat_12h).alias("rn"),
    )
)

grouped = (
    with_trends
    .groupBy("agent_id", "first_known_prior_rows_12h")
    .agg(
        F.min("agent_id_prior_rows_12h_min_prior_csat_12h").alias("min_agent_id_prior_rows_12h_min_prior_csat_12h"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("agent_id_prior_rows_12h_min_prior_csat_12h").alias("sum_agent_id_prior_rows_12h_min_prior_csat_12h_c"),
    )
    .filter(F.col("sum_agent_id_prior_rows_12h_min_prior_csat_12h_c") > 62)
    .drop("sum_agent_id_prior_rows_12h_min_prior_csat_12h_c")
)

derived = (
    grouped
    .filter(F.col("agent_id") <= 3)
    .select(
        "*",
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket_2"),
        F.abs(F.col("distinct_ids") - 4).alias("distinct_ids_distance"),
    )
)

result = (
    derived
    .groupingSets(
        [["distinct_ids", "distinct_ids_bucket_2", "agent_id"], ["distinct_ids"], ["distinct_ids_bucket_2"]],
        "distinct_ids", "distinct_ids_bucket_2", "agent_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.percentile_approx("first_known_prior_rows_12h", 0.5).alias("median_first_known_prior_rows_12h"),
        F.max("first_known_prior_rows_12h").alias("max_first_known_prior_rows_12h"),
    )
    .orderBy(
        F.col("distinct_ids").asc_nulls_last(),
        F.col("distinct_ids_bucket_2").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)

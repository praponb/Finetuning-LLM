from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_team_len_by_team_len = Window.partitionBy("level", "team_len").orderBy(F.col("team_len").asc_nulls_last(), "id")
w_all_by_team_len_running = Window.orderBy("team_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_team_or_unknown = Window.partitionBy("team", "team_or_unknown")
w_team_or_unknown_by_level_pri = Window.partitionBy("team_or_unknown").orderBy(F.col("level").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_team_or_unknown_by_level = Window.partitionBy("team_or_unknown").orderBy(F.col("level").asc_nulls_last(), "id")

scoped = (
    df_adjusters
    .filter((F.col("level") > 3) & (F.col("team").isin("Health", "Motor", "Property")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("adjuster_name")), " ", "_")).alias("adjuster_name_slug"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "team",
        "level",
        "team_len",
        F.row_number().over(w_level_team_len_by_team_len).alias("rn"),
        F.last("team_len", ignorenulls=True).over(w_all_by_team_len_running).alias("last_known_team_len"),
        F.count(F.lit(1)).over(w_team_team_or_unknown).alias("rows_in_team_team_or_unknown"),
        F.sum("team_len").over(w_team_or_unknown_by_level_pri).alias("team_len_sum_6_prior"),
        F.round(F.cume_dist().over(w_team_or_unknown_by_level), 3).alias("level_cume_dist"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.when(F.col("rn") >= 8, "large").when(F.col("rn") >= 3, "medium").otherwise("small").alias("rn_tier"),
        F.lower(F.trim(F.col("team"))).alias("team_clean"),
    )
)

result = (
    labeled
    .select(
        "id",
        F.col("rn").alias("rn_v"),
        F.col("rows_in_team_team_or_unknown").alias("m_rows_in_team_team_or_unknown"),
    )
    .unpivot(
        ids=["id"],
        values=["rn_v", "m_rows_in_team_team_or_unknown"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "metric")
)

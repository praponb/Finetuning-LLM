from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_runn = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_start_date_year_by_en = Window.partitionBy("status", F.year("start_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_product = Window.partitionBy("status", "product")

recent_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-07-19").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

filtered = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score").between(0.1, 0.8)) & (F.col("p.region") != "South"), "left_anti")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("premium").alias("max_policy_premium"),
        F.sum("premium").alias("total_policy_premium"),
        F.min("premium").alias("min_policy_premium"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.max_policy_premium",
        "p.total_policy_premium",
        "p.min_policy_premium",
    )
)

with_windows = (
    summarized_rows
    .select(
        "*",
        F.sum("max_policy_premium").over(w_holder_id_by_start_date_runn).alias("running_max_policy_premium"),
        F.first("end_date", ignorenulls=True).over(w_status_start_date_year_by_en).alias("first_known_end_date"),
        F.size(F.collect_set("holder_id").over(w_status_product)).alias("distinct_holder_id_in_status_product"),
        F.sum("max_policy_premium").over(w_status_start_date_year_by_en).alias("running_max_policy_premium_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("running_max_policy_premium_2").alias("m_running_max_policy_premium_2"),
        F.col("premium").alias("m_premium"),
    )
    .unpivot(
        ids=["id"],
        values=["m_running_max_policy_premium_2", "m_premium"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

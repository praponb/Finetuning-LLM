from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status", "holder_id").orderBy(F.col("avg_premium").desc(), F.col("id").desc())

base = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

holder_id_stats = (
    base
    .groupBy("holder_id")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    base.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

latest_per_key = (
    vs_group
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        F.col("premium_minus_avg").alias("m_premium_minus_avg"),
        F.col("avg_premium").alias("m_avg_premium"),
        F.col("premium").alias("m_premium"),
    )
    .unpivot(
        ids=["id"],
        values=["m_premium_minus_avg", "m_avg_premium", "m_premium"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("id", "metric_name")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("end_date").desc(), F.col("id").desc())

valid_policys = (
    df_policies
    .filter((F.col("status") == "LAPSED") & (F.col("end_date") < F.lit("2026-07-11").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.when(F.col("product").isin("HOME", "AUTO"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        F.when(F.col("status").isin("LAPSED", "CANCELLED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

latest = (
    valid_policys
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

per_group = (
    latest
    .filter(F.col("status_group").isin("other", "group_b"))
    .groupBy(
        "status",
        "holder_id",
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("status") == "LAPSED", 1)) / F.count("*"), 1).alias("pct_status_lapsed"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

detail_rows_policys = (
    per_group.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "inner")
    .select(
        "s.status",
        "s.holder_id",
        "s.end_date_ym",
        "s.row_count",
        "s.distinct_ids",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    detail_rows_policys
    .select(
        "status",
        "holder_id",
        "row_count",
        "distinct_ids",
    )
    .unpivot(
        ids=["status", "holder_id"],
        values=["row_count", "distinct_ids"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("status", "holder_id", "metric")
)

from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("premium") >= 2096.6)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_details_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_details_holder
    .select("id", "premium", "premium_adj")
    .unpivot(
        ids=["id"],
        values=["premium", "premium_adj"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)

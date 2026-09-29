from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2026-10-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
    )
)

with_details_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.end_date_dayofweek",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_parent = (
    with_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.end_date_dayofweek",
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.segment").alias("holder_segment_2"),
    )
)

result = (
    with_parent
    .filter(F.col("product").isin("AUTO", "HOME", "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "end_date_dayofweek",
        "holder_holder_name",
    )
    .orderBy("end_date_dayofweek", "id")
    .limit(10)
)

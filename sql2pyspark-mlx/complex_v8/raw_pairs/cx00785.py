from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "product_or_unknown").orderBy(F.col("start_date").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter((F.col("premium").between(1042.8, 4670.9)) & (F.col("status") != "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

qualified = (
    cleaned_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date").between(F.lit("2025-06-08").cast("date"), F.lit("2026-11-25").cast("date"))) & (F.col("p.start_date") >= F.lit("2024-05-07").cast("date")), "left_semi")
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2026-09-19").cast("date")))
)

scored = (
    qualified
    .select(
        "*",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

unique_rows = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent_holder = (
    unique_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.days_start_date_to_end_date",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

scored_2 = (
    with_parent_holder
    .filter(F.col("start_date").isNotNull())
    .select(
        "*",
        F.concat_ws("-", F.col("holder_region"), F.col("holder_segment")).alias("holder_region_holder_segment_label"),
        F.add_months(F.col("start_date"), 6).alias("start_date_plus_6m"),
    )
)

result = (
    scored_2
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "days_start_date_to_end_date",
        "holder_joined_on",
        "start_date_plus_6m",
    )
    .orderBy(F.col("holder_id").asc_nulls_last())
)

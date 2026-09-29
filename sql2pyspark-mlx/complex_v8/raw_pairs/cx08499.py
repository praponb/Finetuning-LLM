from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_region").orderBy(F.col("total_premium").desc(), F.col("holder_id").desc())

prepared_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

grouped = (
    prepared_policys
    .groupBy("holder_id")
    .agg(
        F.sum("premium").alias("total_premium"),
        F.sum("days_start_date_to_end_date").alias("total_days_start_date_to_end_date"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
)

without_matches = (
    grouped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isin("CANCELLED", "LAPSED")) & (F.col("p.end_date").between(F.lit("2026-01-23").cast("date"), F.lit("2026-11-25").cast("date"))), "left_semi")
)

with_details_holder = (
    without_matches.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.holder_id",
        "s.total_premium",
        "s.distinct_holder_ids",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

unique_rows = (
    with_details_holder
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupingSets(
        [["distinct_holder_ids", "holder_region", "holder_id"], ["distinct_holder_ids"], []],
        "distinct_holder_ids", "holder_region", "holder_id",
    )
    .agg(
        F.countDistinct("holder_id").alias("distinct_holder_ids_2"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("distinct_holder_ids").asc_nulls_last(),
        F.col("holder_region").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
    )
)

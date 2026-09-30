from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_end_date_month = Window.partitionBy("product", F.month("end_date"))
w_status_by_start_date = Window.partitionBy("status").orderBy("start_date", "id")

prepared = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

windowed = (
    prepared
    .withColumn("product_end_date_month_max_premium", F.max("premium").over(w_product_end_date_month))
    .withColumn("seq_no", F.row_number().over(w_status_by_start_date))
)

with_parent_holder = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.seq_no",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

filtered = (
    with_parent_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("G%")), "left_semi")
)

filtered_2 = (
    filtered
    .crossJoin(filtered.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

combined_details = (
    filtered_2.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on") < F.lit("2025-04-20").cast("date"))
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.seq_no",
        "s.holder_joined_on",
        F.col("p.joined_on").alias("holder_joined_on_2"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
    .filter(F.col("holder_id").isNotNull())
)

result = (
    screened
    .groupBy("holder_id", "start_date")
    .pivot("status", ["EXPIRED", "LAPSED", "ACTIVE"])
    .agg(F.max("seq_no").alias("max_v"), F.count("seq_no").alias("n"))
    .orderBy("holder_id", "start_date")
)

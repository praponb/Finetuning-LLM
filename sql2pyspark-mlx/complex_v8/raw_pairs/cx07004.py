from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("status").isin("EXPIRED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
    )
)

enriched_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.joined_on").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.end_date_label",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    enriched_holder
    .groupBy("end_date")
    .pivot("product", ["TRAVEL", "HOME", "HEALTH"])
    .agg(F.min("premium"))
)

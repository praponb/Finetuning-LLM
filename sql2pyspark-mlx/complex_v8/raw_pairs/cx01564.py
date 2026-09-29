from pyspark.sql import functions as F

cleaned_policys = df_policies.filter(F.col("end_date") < F.lit("2026-04-25").cast("date"))

enriched = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select("s.*", F.col("p.joined_on").alias("holder_joined_on"))
)

result = (
    enriched
    .filter(F.col("start_date") >= F.lit("2024-12-03").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
    )
    .orderBy("holder_id", "id")
    .limit(10)
)

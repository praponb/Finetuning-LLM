from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.date_trunc("year", F.col("joined_on")).alias("joined_on_year"),
    )
)

holder_side = (
    cleaned_holders
    .filter(F.col("holder_name").isNotNull())
    .select(F.col("holder_name").alias("label"))
)

holder_rows = (
    df_policyholders
    .filter((F.col("region") != "North") & (F.col("risk_score") > 0.3))
    .select("segment")
)

result = (
    holder_side
    .subtract(holder_rows)
    .orderBy("label")
)

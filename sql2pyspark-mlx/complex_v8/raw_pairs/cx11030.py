from pyspark.sql import functions as F

cleaned = (
    df_adjusters
    .filter((F.col("team") == "Property") & (F.upper(F.trim(F.col("adjuster_name"))).like("F%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

adjuster_rows = cleaned.select("adjuster_name")

claim_side = df_claims.select("status")

result = adjuster_rows.subtract(claim_side)

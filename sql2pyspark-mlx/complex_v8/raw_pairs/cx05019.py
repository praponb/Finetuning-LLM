from pyspark.sql import functions as F

holder_side = df_policyholders.select("id")

holder_rows = (
    df_policyholders
    .filter((F.col("joined_on") < F.lit("2025-09-02").cast("date")) & (F.col("risk_score") < 0.8))
    .select("id")
)

result = (
    holder_side
    .subtract(holder_rows)
    .orderBy("id")
)

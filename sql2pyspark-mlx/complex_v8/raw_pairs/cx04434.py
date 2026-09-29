from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("joined_on") < F.lit("2025-07-02").cast("date"))
    .select("id")
)

holder_side_2 = df_policyholders.select("id")

result = holder_side.subtract(holder_side_2)

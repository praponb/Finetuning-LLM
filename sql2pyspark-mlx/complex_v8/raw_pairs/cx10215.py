from pyspark.sql import functions as F

holder_rows = df_policyholders.select("id")

policy_side = (
    df_policies
    .filter((F.col("premium").between(1042.5, 3817.5)) & (F.col("start_date") >= F.lit("2024-06-19").cast("date")))
    .select("holder_id")
)

result = (
    holder_rows
    .subtract(policy_side)
    .orderBy("id")
)

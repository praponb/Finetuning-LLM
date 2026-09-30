from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-07-01", "2023-07-14") | (F.col("plan") == "basic"))
    .select("id", "monthly_fee", "end_date")
)

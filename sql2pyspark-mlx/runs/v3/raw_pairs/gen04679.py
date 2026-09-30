from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2023)
    .select("plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") <= "2024-07-01")
        .select("plan")
    )
)

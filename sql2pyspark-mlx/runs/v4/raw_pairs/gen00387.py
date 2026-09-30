from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]))
    .select("end_date", "start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") < "2023-10-01")
        .select("end_date", "start_date")
    )
    .orderBy("start_date")
)

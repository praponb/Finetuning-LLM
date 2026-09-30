from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "enterprise", "basic"]))
    .select("id", "plan", "end_date")
    .intersect(
        df_subscriptions
        .filter(F.year("end_date") >= 2024)
        .select("id", "plan", "end_date")
    )
    .orderBy("end_date")
)

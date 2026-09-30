from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("source"))
    .union(
        df_subscriptions
        .filter(~F.col("plan").isin(["basic", "standard"]))
        .select("start_date", F.lit("subscription").alias("source"))
    )
)

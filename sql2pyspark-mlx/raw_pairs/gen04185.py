from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-01-01")
    .select(
        "id",
        "end_date",
        "status",
        "start_date",
        F.when(F.col("plan").isin(["enterprise", "basic"]), "Core").otherwise("Other").alias("plan_group"),
        F.quarter("start_date").alias("start_date_quarter"),
    )
    .orderBy(F.col("plan_group").desc(), "id")
    .limit(50)
)

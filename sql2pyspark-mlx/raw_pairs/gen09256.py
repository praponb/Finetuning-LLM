from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") | F.col("monthly_fee").between(90, 180))
    .select(
        "id",
        "monthly_fee",
        "end_date",
        "plan",
        F.when(F.col("plan").isin(["premium", "standard"]), "Group A").otherwise("Other").alias("plan_group"),
    )
    .orderBy(F.col("end_date").desc())
)

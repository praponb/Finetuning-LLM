from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "plan",
        "end_date",
        F.when(F.col("plan") == "standard", 1).otherwise(0).alias("is_standard"),
    )
    .orderBy(F.col("plan").desc())
)

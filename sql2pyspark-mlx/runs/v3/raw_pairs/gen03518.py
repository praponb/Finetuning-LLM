from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 170) & F.col("customer_id").isNotNull() & (F.col("plan") == "premium"))
    .select(
        "id",
        "monthly_fee",
        "status",
        "end_date",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
    .orderBy(F.col("plan_title").desc())
)

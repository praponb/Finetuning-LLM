from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter((F.col("holder_id") <= 9) | (F.col("end_date") >= F.lit("2025-11-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

screened = (
    valid_policys
    .crossJoin(valid_policys.agg(F.max("premium").alias("overall_max_premium")))
    .filter(F.col("premium") < F.col("overall_max_premium"))
    .drop("overall_max_premium")
)

result = (
    screened
    .groupBy("end_date")
    .pivot("product", ["LIFE", "AUTO", "HOME", "TRAVEL"])
    .agg(F.max("premium"))
    .orderBy("end_date")
)

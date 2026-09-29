from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 24)
    .select(
        "browser",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
    .orderBy("is_mobile", "browser")
)

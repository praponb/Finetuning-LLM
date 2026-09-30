from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Chrome", "Safari"]) & F.col("country").isin(["JP", "US"]))
    .select(
        "id",
        "country",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
)

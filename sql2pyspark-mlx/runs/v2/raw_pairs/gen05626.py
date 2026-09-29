from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "Consumer"]) | ~F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .select(
        "id",
        "segment",
        "signup_date",
        "city",
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
)

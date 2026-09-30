from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("credit_limit") > 14300) & F.col("city").isin(["London", "New York", "Bangkok"])) | F.col("is_active"))
    .select(
        "id",
        "country",
        "credit_limit",
        "city",
        F.lower("segment").alias("segment_lower"),
    )
)

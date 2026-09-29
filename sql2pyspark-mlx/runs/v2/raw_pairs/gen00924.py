from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("is_active") & (F.upper(F.trim("name")) == "HENRY FORD")) | ~F.col("country").isin(["UK", "US", "DE"]))
    .select(
        "country",
        "email",
        "city",
        F.round("credit_limit", 1).alias("credit_limit_rounded"),
    )
)

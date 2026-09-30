from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB"]))
    .select(
        "city",
        "name",
        F.round((F.col("age") / F.col("credit_limit")), 2).alias("age_per_credit_limit"),
    )
    .orderBy("age_per_credit_limit")
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Berlin", "Tokyo"]))
    .select(
        "country",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
    )
    .orderBy("country")
)

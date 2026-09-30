from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("credit_limit") < 7400) & F.col("country").isin(["DE", "TH"]))
    .select(
        "id",
        "credit_limit",
        F.when(F.col("credit_limit") >= 14600, "High").when(F.col("credit_limit") >= 5600, "Medium").otherwise("Low").alias("credit_limit_band"),
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc(), F.col("credit_limit_band").desc())
)

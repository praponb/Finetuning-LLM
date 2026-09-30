from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNull() | F.col("is_active"))
    .select(
        "is_active",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
    )
    .orderBy("signup_date_plus_months")
)

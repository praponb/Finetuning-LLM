from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "credit_limit",
        "is_active",
        "city",
        F.upper(F.trim("name")).alias("name_normalized"),
    )
)

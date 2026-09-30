from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(11700, 15500) & F.col("email").like("%@startup.io") & F.col("name").like("%Kanya%"))
    .select(
        "id",
        "email",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
    )
)

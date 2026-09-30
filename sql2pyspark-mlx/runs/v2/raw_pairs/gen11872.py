from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 9500)
    .withColumn("email_filled", F.coalesce("email", F.lit("unassigned")))
    .withColumn("email_filled_2", F.coalesce("email", F.lit("unassigned")))
)

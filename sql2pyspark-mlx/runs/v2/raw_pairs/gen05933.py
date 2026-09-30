from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & F.col("email").like("%@company.co"))
    .select("id", "segment")
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull() & F.col("country").isin(["JP", "TH", "UK"]) & ~F.col("email").like("%@yahoo.com"))
    .select("id", "credit_limit")
    .orderBy("id")
    .limit(50)
)

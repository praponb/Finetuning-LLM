from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co") & F.col("age").between(36, 57))
    .select("id", "name", F.initcap(F.trim("country")).alias("country_title"))
    .orderBy(F.col("name").desc())
)

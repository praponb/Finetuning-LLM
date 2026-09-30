from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(103000, 107000) & ~F.col("email").like("%@GMAIL.COM"))
    .select("id", "is_remote", F.substring("email", 1, 3).alias("email_prefix"))
)

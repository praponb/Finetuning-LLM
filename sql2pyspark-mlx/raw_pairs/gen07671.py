from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["SG", "UK"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .filter(F.col("credit_limit_range") >= 28)
    .orderBy("country_group")
)

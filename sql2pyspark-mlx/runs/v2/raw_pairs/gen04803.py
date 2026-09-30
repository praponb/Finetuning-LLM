from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & ~F.col("country").isin(["UK", "DE"]) & ~F.col("email").like("%@GMAIL.COM"))
    .withColumn("country_group", F.when(F.col("country").isin(["US", "DE"]), "Group A").otherwise("Other"))
)
